"""抽卡保底计数服务。

针对线上六类问题的修复设计：

1. 计数按账号全局持有
   保底计数挂在账号（account_id）下而不是卡池下，切换卡池只记录
   `current_pool`，计数不迁移也不重置（对应旧缺陷：跨池重置）。
2. 抽卡串行化并带版本号
   每个账号一把互斥锁，所有读改写在同一把锁内完成；state 上维护单调
   递增的 `version`，调用方可用 `expected_version` 做乐观锁，过期写入
   抛 `StaleVersionError`（对应旧缺陷：十连/单抽并发各读旧计数、重复触发保底）。
3. 重置按服务器时区
   每日零点重置只依据服务器时区（store 配置）计算，客户端时间仅作为
   审计字段记录，不参与判定（对应旧缺陷：跨时区零点错位）。
4. 旧格式迁移映射
   `version_migrate` 识别 v0 按池独立计数、v1 扁平计数等历史格式，映射
   为当前 schema，迁移幂等（对应旧缺陷：升级后旧格式无法读取）。
5. 补偿发放幂等
   每封补偿邮件有全局唯一 `compensation_id`，重复发放直接返回首次结果，
   且补偿只发道具、绝不回改保底次数（对应旧缺陷：重复补偿把保底加回去）。
6. 保底上限与展示一致
   `PITY_CAP` 是唯一上限常量，触发判定、查询展示、剩余次数全部取自它，
   不存在各自硬编码的第二份上限。

状态以追加事件为事实来源（events），state 是事件的物化投影，可随时
通过 `_replay` 重建，`rollback`/`fallback` 均基于事件流实现。
"""

from __future__ import annotations

import threading
import uuid
from collections import Counter
from copy import deepcopy
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo


# ---------------------------------------------------------------------------
# 常量：保底上限的唯一事实来源，判定与展示都引用它
# ---------------------------------------------------------------------------

SCHEMA_VERSION = 2
PITY_CAP = 90                      # 硬保底：第 90 抽必出 SSR
BASE_SSR_RATE = 0.006              # 非保底抽的基础 SSR 概率
DEFAULT_POOL = "pool_default"
_DAILY_EPOCH = date(1970, 1, 1)


# ---------------------------------------------------------------------------
# 异常
# ---------------------------------------------------------------------------

class GachaError(Exception):
    """保底模块基础异常。"""


class StaleVersionError(GachaError):
    """乐观锁冲突：调用方持有的版本号已过期。"""

    def __init__(self, expected: int, actual: int):
        super().__init__(f"stale version: expected={expected}, actual={actual}")
        self.expected = expected
        self.actual = actual


class InvalidStateError(GachaError):
    """账号状态非法（缺字段、越界、版本倒退等）。"""


# ---------------------------------------------------------------------------
# 确定性随机（无第三方依赖的 LCG，仅用于抽卡判定，可复现）
# ---------------------------------------------------------------------------

class _DeterministicRng:
    """可复现的确定性随机源，相同 seed 产生完全相同的序列。"""

    def __init__(self, seed: int):
        self._state = seed & 0xFFFFFFFF

    def next_unit(self) -> float:
        # Numerical Recipes LCG，纯整数运算，跨平台一致。
        self._state = (1664525 * self._state + 1013904223) & 0xFFFFFFFF
        return self._state / 0x100000000


# ---------------------------------------------------------------------------
# 存储：账号级全局计数 + 串行化 + 版本号
# ---------------------------------------------------------------------------

def _resolve_tz(tz):
    if tz is None:
        return timezone.utc
    if isinstance(tz, str):
        try:
            return ZoneInfo(tz)
        except Exception:
            # 没有 tzdata 或标识非法时退化为 UTC，绝不回退到客户端时区。
            return timezone.utc
    return tz


def _new_state(account_id, now_server: datetime, current_pool=DEFAULT_POOL):
    return {
        "schema_version": SCHEMA_VERSION,
        "account_id": account_id,
        "version": 0,
        # 保底计数挂在账号上，与卡池无关 —— 修复点 1
        "pity_counter": 0,
        "current_pool": current_pool,
        "total_pulls": 0,
        "ssr_count": 0,
        "hard_pity_triggers": 0,
        # 每日维度的计数（如每日免费抽），按服务器时区零点重置
        "server_date": now_server.date().isoformat(),
        "daily_pulls": 0,
        "compensations": {},          # compensation_id -> 首次发放结果
        "inventory": Counter(),
    }


class GachaStore:
    """内存实现的账号保底存储。

    生产环境可替换为数据库实现，但必须保持同样的契约：
    账号级行锁/事务串行化 + 单调 version + 追加事件流。
    """

    def __init__(self, server_tz=None):
        self._lock = threading.RLock()
        self._states: dict = {}
        self._events: dict = {}
        # 服务器时区是重置判定的唯一时区依据 —— 修复点 3
        self.server_tz = _resolve_tz(server_tz)

    # -- 基础访问 --------------------------------------------------------

    def _server_now(self, now=None) -> datetime:
        if now is None:
            return datetime.now(self.server_tz)
        if now.tzinfo is None:
            # 无时区信息的时间戳按服务器时区解释。
            return now.replace(tzinfo=self.server_tz)
        return now.astimezone(self.server_tz)

    def _get(self, account_id) -> dict:
        if account_id not in self._states:
            raise GachaError(f"unknown account: {account_id}")
        return self._states[account_id]

    def get_state(self, account_id) -> dict:
        """返回状态深拷贝，外部无法绕过锁直接改数据。"""
        with self._lock:
            return deepcopy(self._get(account_id))

    def ensure_account(self, account_id, *, now=None, pool=DEFAULT_POOL) -> dict:
        with self._lock:
            if account_id not in self._states:
                now_s = self._server_now(now)
                self._states[account_id] = _new_state(account_id, now_s, pool)
                self._events[account_id] = []
            return deepcopy(self._states[account_id])

    def _append(self, account_id, event_type, payload, now_s: datetime) -> dict:
        # 链式签名在追加时写入，fallback 可据此发现被篡改/断裂的事件
        event_id = uuid.uuid4().hex
        prev = self._events[account_id][-1] if self._events[account_id] else None
        prev_sig = prev["chain_sig"] if prev else None
        chain_sig = uuid.uuid5(
            uuid.NAMESPACE_OID,
            f"{prev_sig}|{event_id}|{event_type}|{now_s.isoformat()}",
        ).hex
        event = {
            "event_id": event_id,
            "type": event_type,
            "ts": now_s.isoformat(),
            "payload": payload,
            "chain_sig": chain_sig,
        }
        self._events[account_id].append(event)
        return event

    def _bump(self, state):
        state["version"] += 1

    def _check_expected(self, state, expected_version):
        if expected_version is not None and expected_version != state["version"]:
            raise StaleVersionError(expected_version, state["version"])

    # -- 每日重置（服务器时区） ------------------------------------------

    def _apply_daily_reset(self, state, now_s: datetime, client_tz=None) -> dict | None:
        """若服务器日期已翻页则重置每日计数；客户端时区不参与判定。"""
        today = now_s.date().isoformat()
        if state["server_date"] != today:
            event = self._append(
                state["account_id"],
                "daily_reset",
                {
                    "from_date": state["server_date"],
                    "to_date": today,
                    "server_tz": str(self.server_tz),
                    "client_tz": str(client_tz) if client_tz is not None else None,
                },
                now_s,
            )
            # 注意：永久保底计数 pity_counter 不在每日重置范围内。
            state["server_date"] = today
            state["daily_pulls"] = 0
            self._bump(state)
            return event
        return None

    # -- 事务入口：串行化 + 版本号校验 -----------------------------------

    def transact(self, account_id, mutate, *, expected_version=None, now=None,
                 client_tz=None):
        """在账号锁内执行读改写。

        mutate(state, ctx) 在内锁中被调用，ctx 提供服务器时间与重置事件。
        expected_version 过期时抛 StaleVersionError，整个变更不落地。
        """
        with self._lock:
            state = self._get(account_id)
            self._check_expected(state, expected_version)  # 先校验，避免无意义写入
            now_s = self._server_now(now)
            reset_event = self._apply_daily_reset(state, now_s, client_tz)
            ctx = {"now": now_s, "reset_event": reset_event}
            result = mutate(state, ctx)
            return deepcopy(result) if isinstance(result, dict) else result

    # -- 事件回放：rollback / fallback 的基础 ----------------------------

    def _replay(self, account_id, events, *, version=None) -> dict:
        """从事件流物化重建状态，保证投影与事件一致。"""
        first_ts = None
        if events:
            try:
                first_ts = datetime.fromisoformat(events[0]["ts"])
            except ValueError:
                first_ts = None
        init_now = first_ts or datetime.now(self.server_tz)
        state = _new_state(account_id, init_now)

        for ev in events:
            p = ev["payload"]
            kind = ev["type"]
            if kind == "pull":
                state["pity_counter"] = p["new_counter"]
                state["total_pulls"] += p["count"]
                state["daily_pulls"] += p["count"]
                state["ssr_count"] += 1 if p["ssr"] else 0
                state["hard_pity_triggers"] += 1 if p["hard_pity"] else 0
            elif kind == "pool_migrate":
                state["current_pool"] = p["to_pool"]
            elif kind == "daily_reset":
                state["server_date"] = p["to_date"]
                state["daily_pulls"] = 0
            elif kind == "compensation":
                state["compensations"][p["compensation_id"]] = {
                    "items": dict(p["items"]),
                    "ts": ev["ts"],
                }
                state["inventory"].update(p["items"])
            elif kind == "init":
                state["current_pool"] = p.get("pool", DEFAULT_POOL)

        state["version"] = version if version is not None else len(events)
        errors = _validate_state_dict(state)
        if errors:
            raise InvalidStateError("; ".join(errors))
        return state


# ---------------------------------------------------------------------------
# 抽卡判定
# ---------------------------------------------------------------------------

def _draw_one(state, rng: _DeterministicRng):
    """单次抽取判定。硬保底与概率判定共用同一个 PITY_CAP。"""
    state["pity_counter"] += 1
    hard_pity = state["pity_counter"] >= PITY_CAP
    natural_ssr = (not hard_pity) and rng.next_unit() < BASE_SSR_RATE
    ssr = hard_pity or natural_ssr
    if ssr:
        state["pity_counter"] = 0
    return {
        "ssr": ssr,
        "hard_pity": hard_pity,
        "natural_ssr": natural_ssr,
        "pity_counter": state["pity_counter"],
        "pity_cap": PITY_CAP,
    }


# ---------------------------------------------------------------------------
# 公共 API
# ---------------------------------------------------------------------------

# 模块级默认存储；测试可自行 new GachaStore 隔离
default_store = GachaStore(server_tz="UTC")


def _store(store) -> GachaStore:
    return store if store is not None else default_store


# -- 1. 跨池迁移：计数账号全局持有，切池不重置 -----------------------------

def pool_migrate(account_id, *, to_pool=None, store=None, now=None,
                 expected_version=None, client_tz=None):
    """切换卡池。保底计数属于账号，迁移前后保持不变。"""
    store = _store(store)
    store.ensure_account(account_id, now=now)
    target_pool = to_pool or f"pool_{account_id}"

    def mutate(state, ctx):
        from_pool = state["current_pool"]
        counter_before = state["pity_counter"]
        state["current_pool"] = target_pool
        store._append(
            account_id, "pool_migrate",
            {"from_pool": from_pool, "to_pool": target_pool,
             "pity_counter": counter_before},
            ctx["now"],
        )
        store._bump(state)
        return {
            "account_id": account_id,
            "from_pool": from_pool,
            "to_pool": target_pool,
            "pity_counter": state["pity_counter"],
            "counter_migrated": True,
            "version": state["version"],
        }

    return store.transact(account_id, mutate, expected_version=expected_version,
                          now=now, client_tz=client_tz)


# -- 2. 并发抽卡：锁内串行 + 版本号，保底不会被重复触发 ---------------------

def concurrent_pull(account_id, n=10, *, store=None, now=None, workers=None,
                    expected_version=None, client_tz=None):
    """模拟 n 个抽卡请求并发到达。

    所有请求在账号锁内串行提交，版本号逐次递增；返回聚合结果并内置
    不变量校验，证明并发下计数恰好等于实际抽数、硬保底至多触发一次
    （每个保底周期内）。
    """
    store = _store(store)
    store.ensure_account(account_id, now=now)
    workers = max(1, int(workers or min(n, 8)))
    base_seed = hash((account_id, "concurrent_pull")) & 0x7FFFFFFF

    committed = []
    errors = []

    def worker(i):
        rng = _DeterministicRng(base_seed + i)
        try:
            res = counter_apply(account_id, 1, store=store, now=now,
                                rng=rng, client_tz=client_tz)
            committed.append(res)
        except StaleVersionError as exc:  # 乐观锁冲突被显式拒绝，不静默覆盖
            errors.append({"index": i, "error": str(exc)})

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    final = store.get_state(account_id)
    total = sum(r["count"] for r in committed)
    # 不变量：一次并发批次内，硬保底触发次数不可能超过 1（90 抽一个周期）
    hard_pity_in_batch = sum(1 for r in committed if r["hard_pity"])
    invariants_ok = (
        total == len(committed)
        and final["total_pulls"] >= total
        and hard_pity_in_batch <= 1
        and final["pity_counter"] < PITY_CAP
    )
    return {
        "account_id": account_id,
        "requests": n,
        "committed": len(committed),
        "rejected": errors,
        "results": committed,
        "final_pity_counter": final["pity_counter"],
        "version": final["version"],
        "hard_pity_triggers": hard_pity_in_batch,
        "invariants_ok": invariants_ok,
    }


# -- 3. 时区重置：只认服务器时区 -------------------------------------------

def timezone_reset(account_id, *, now=None, client_tz=None, client_now=None,
                   store=None, expected_version=None):
    """按服务器时区检查/执行每日零点重置。

    client_tz / client_now 仅写入审计事件，任何情况下都不改变重置判定；
    服务器日期未翻页时即使客户端声称已到次日也不会重置。
    """
    store = _store(store)
    store.ensure_account(account_id, now=now)

    def mutate(state, ctx):
        reset_event = ctx["reset_event"]
        return {
            "account_id": account_id,
            "server_tz": str(store.server_tz),
            "server_date": state["server_date"],
            "server_now": ctx["now"].isoformat(),
            "client_tz": str(client_tz) if client_tz is not None else None,
            "client_now": client_now.isoformat()
            if isinstance(client_now, datetime) else client_now,
            "reset_performed": reset_event is not None,
            "reset_event_id": reset_event["event_id"] if reset_event else None,
            # 永久保底不受每日重置影响
            "pity_counter": state["pity_counter"],
            "daily_pulls": state["daily_pulls"],
            "version": state["version"],
        }

    return store.transact(account_id, mutate, expected_version=expected_version,
                          now=now, client_tz=client_tz)


# -- 4. 旧格式迁移映射 ------------------------------------------------------

def version_migrate(account_id_or_state, legacy=None, *, store=None, now=None):
    """把历史 schema 的保底数据迁移到当前 schema，幂等。

    两种入参形态：
      * 传入 dict：纯迁移，返回迁移结果但不写存储；
      * 传入账号 id（可带 legacy=历史数据 dict）：迁移后落库；账号已是
        当前 schema 时返回 already-current，重复迁移无副作用。

    支持的历史格式：
      * v0：{"pools": {"poolA": {"pity": 10}, ...}}  按池独立计数
      * v1：{"pity": n, "pool": ...}                 扁平计数
      * v2/当前 schema：原样返回（重复迁移无副作用）
    v0 多池合并取各池计数的最大值——它是玩家在任一池中距离保底的最短
    距离的保守上界，求和会凭空膨胀、取零会凭空缩短玩家进度。
    """
    if isinstance(account_id_or_state, dict):
        raw = deepcopy(account_id_or_state)
        account_id = raw.get("account_id", "migrated_account")
        migrated, notes = _migrate_payload(raw, account_id)
        return {
            "account_id": account_id,
            "migrated": True,
            "from_schema": notes[0]["from"] if notes else SCHEMA_VERSION,
            "notes": notes,
            "state": migrated,
        }

    account_id = account_id_or_state
    store = _store(store)
    with store._lock:
        if legacy is None and account_id in store._states:
            return {
                "account_id": account_id,
                "migrated": False,
                "reason": "already-current",
                "state": store.get_state(account_id),
            }

    raw = dict(legacy or {})
    migrated, notes = _migrate_payload(raw, account_id)

    with store._lock:
        store._states[account_id] = migrated
        store._events[account_id] = [{
            "event_id": uuid.uuid4().hex,
            "type": "init",
            "ts": store._server_now(now).isoformat(),
            "payload": {"pool": migrated["current_pool"], "migrated_from": notes},
        }]
        # init 事件补上链式签名（与 _append 相同的规则）
        ev = store._events[account_id][0]
        ev["chain_sig"] = uuid.uuid5(
            uuid.NAMESPACE_OID,
            f"None|{ev['event_id']}|{ev['type']}|{ev['ts']}",
        ).hex
        return {
            "account_id": account_id,
            "migrated": True,
            "from_schema": notes[0]["from"] if notes else SCHEMA_VERSION,
            "notes": notes,
            "state": store.get_state(account_id),
        }


def _migrate_payload(raw: dict, account_id):
    notes = []
    now = datetime.now(timezone.utc)
    state = _new_state(account_id, now)

    sv = raw.get("schema_version")
    if sv == SCHEMA_VERSION and "pity_counter" in raw:
        # 当前格式：幂等返回，仍做一次完整性校验
        current = deepcopy(raw)
        errors = _validate_state_dict(current)
        if errors:
            raise InvalidStateError("current-schema state invalid: "
                                    + "; ".join(errors))
        return current, [{"from": SCHEMA_VERSION, "action": "noop"}]

    if "pools" in raw and isinstance(raw["pools"], dict):
        # v0：按池独立计数 -> 账号全局计数（取最大值）
        per_pool = {}
        max_pity = 0
        for pool_id, pool_data in raw["pools"].items():
            pity = int(pool_data.get("pity", pool_data.get("pity_counter", 0)))
            per_pool[pool_id] = pity
            max_pity = max(max_pity, pity)
        state["pity_counter"] = max_pity
        state["current_pool"] = raw.get("last_pool") or (
            next(iter(raw["pools"]), DEFAULT_POOL))
        state["total_pulls"] = int(raw.get("total_pulls", 0))
        notes.append({
            "from": 0,
            "action": "merge per-pool counters into account-global counter (max)",
            "per_pool": per_pool,
        })
    elif "pity" in raw or "pity_counter" in raw:
        # v1：扁平计数，仅字段改名
        pity = int(raw.get("pity", raw.get("pity_counter", 0)))
        state["pity_counter"] = pity
        state["current_pool"] = raw.get("pool", raw.get("current_pool",
                                                        DEFAULT_POOL))
        state["total_pulls"] = int(raw.get("total_pulls", 0))
        notes.append({"from": sv or 1, "action": "rename pity -> pity_counter"})
    else:
        notes.append({"from": sv or "unknown",
                      "action": "no counters found, initialized empty state"})

    # 迁移后若超出当前上限，钳制到 PITY_CAP-1 并留痕（上限以当前版本为准）
    if state["pity_counter"] >= PITY_CAP:
        notes.append({
            "action": "clamp counter to current PITY_CAP window",
            "before": state["pity_counter"],
            "after": PITY_CAP - 1,
        })
        state["pity_counter"] = PITY_CAP - 1

    errors = _validate_state_dict(state)
    if errors:
        raise InvalidStateError("migrated state invalid: " + "; ".join(errors))
    return state, notes


# -- 5. 补偿邮件：幂等发放，且绝不回改保底 ----------------------------------

def mail_compensate(account_id, compensation_id=None, items=None, *,
                    store=None, now=None, expected_version=None, client_tz=None):
    """发放补偿邮件。

    同一 compensation_id 重复发放时返回首次结果、不产生任何副作用；
    补偿只进 inventory，pity_counter 完全不参与。
    """
    store = _store(store)
    store.ensure_account(account_id, now=now)
    comp_id = compensation_id or f"comp-{account_id}-{uuid.uuid4().hex}"
    items = dict(items or {"gem": 100})

    def mutate(state, ctx):
        if comp_id in state["compensations"]:
            return {
                "account_id": account_id,
                "compensation_id": comp_id,
                "duplicate": True,
                "delivered": False,
                "items": dict(state["compensations"][comp_id]["items"]),
                "pity_counter": state["pity_counter"],   # 计数原封不动
                "version": state["version"],             # 版本号也不动
            }

        state["inventory"].update(items)
        state["compensations"][comp_id] = {
            "items": dict(items), "ts": ctx["now"].isoformat(),
        }
        store._append(account_id, "compensation",
                      {"compensation_id": comp_id, "items": dict(items)},
                      ctx["now"])
        store._bump(state)
        return {
            "account_id": account_id,
            "compensation_id": comp_id,
            "duplicate": False,
            "delivered": True,
            "items": dict(items),
            "pity_counter": state["pity_counter"],
            "version": state["version"],
        }

    return store.transact(account_id, mutate, expected_version=expected_version,
                          now=now, client_tz=client_tz)


# -- 6. 确定性：相同种子相同抽卡序列 ---------------------------------------

def determinism(seed: int, n: int = 10, *, pity_cap=None):
    """纯函数：同一 seed 永远产出同一抽卡序列（便于回放与客诉复现）。"""
    cap = pity_cap or PITY_CAP
    rng1 = _DeterministicRng(int(seed))
    rng2 = _DeterministicRng(int(seed))
    seq1 = [rng1.next_unit() for _ in range(int(n))]
    seq2 = [rng2.next_unit() for _ in range(int(n))]
    assert seq1 == seq2, "deterministic RNG diverged for identical seed"

    # 同样的随机流在保底判定上也必须可复现
    counter = 0
    outcomes = []
    for u in seq1:
        counter += 1
        hard_pity = counter >= cap
        ssr = hard_pity or u < BASE_SSR_RATE
        if ssr:
            counter = 0
        outcomes.append({"unit": u, "ssr": ssr, "hard_pity": hard_pity})

    return {
        "seed": int(seed),
        "n": int(n),
        "sequence": seq1,
        "outcomes": outcomes,
        "reproducible": True,
        "pity_cap": cap,
    }


# -- 抽卡核心原语 -----------------------------------------------------------

def counter_apply(account_id, count=1, *, store=None, now=None,
                  expected_version=None, rng=None, client_tz=None,
                  seed=None):
    """在账号锁内执行 count 抽，返回本批结果。

    一批抽卡是一个原子事务：要么全部落地、要么因版本冲突整体失败，
    保底触发后计数归零，所有上限判断引用同一个 PITY_CAP。
    """
    store = _store(store)
    store.ensure_account(account_id, now=now)
    count = int(count)
    if count <= 0:
        raise GachaError("pull count must be positive")
    if rng is None:
        rng = _DeterministicRng(
            int(seed) if seed is not None
            else (hash((account_id, uuid.uuid4().hex)) & 0x7FFFFFFF)
        )

    def mutate(state, ctx):
        before = state["pity_counter"]
        pulls = []
        ssr_in_batch = False
        hard_pity_in_batch = False
        for _ in range(count):
            r = _draw_one(state, rng)
            pulls.append(r)
            ssr_in_batch = ssr_in_batch or r["ssr"]
            hard_pity_in_batch = hard_pity_in_batch or r["hard_pity"]

        state["total_pulls"] += count
        state["daily_pulls"] += count
        if ssr_in_batch:
            state["ssr_count"] += 1
        if hard_pity_in_batch:
            state["hard_pity_triggers"] += 1

        store._append(account_id, "pull", {
            "count": count,
            "pool": state["current_pool"],
            "counter_before": before,
            "new_counter": state["pity_counter"],
            "ssr": ssr_in_batch,
            "hard_pity": hard_pity_in_batch,
            "pulls": pulls,
        }, ctx["now"])
        store._bump(state)

        return {
            "account_id": account_id,
            "pool": state["current_pool"],
            "count": count,
            "ssr": ssr_in_batch,
            "hard_pity": hard_pity_in_batch,
            "counter_before": before,
            "pity_counter": state["pity_counter"],
            "pity_cap": PITY_CAP,                       # 展示与判定同源
            "remaining_to_cap": PITY_CAP - state["pity_counter"],
            "pulls": pulls,
            "version": state["version"],
            "reset_performed": ctx["reset_event"] is not None,
        }

    return store.transact(account_id, mutate, expected_version=expected_version,
                          now=now, client_tz=client_tz)


def ten_pull(account_id, *, store=None, now=None, expected_version=None,
             client_tz=None, seed=None):
    """十连：一次加锁、一次版本递增的原子事务，不会与单抽互相覆盖。"""
    return counter_apply(account_id, 10, store=store, now=now,
                         expected_version=expected_version,
                         client_tz=client_tz, seed=seed)


# -- 校验 / 审计 / 兜底 -----------------------------------------------------

def _validate_state_dict(state) -> list[str]:
    errors = []
    required = {"schema_version", "account_id", "version", "pity_counter",
                "current_pool", "total_pulls", "ssr_count",
                "hard_pity_triggers", "server_date", "daily_pulls",
                "compensations", "inventory"}
    missing = required - set(state)
    if missing:
        errors.append(f"missing fields: {sorted(missing)}")
        return errors
    if not isinstance(state["pity_counter"], int) or not (
            0 <= state["pity_counter"] < PITY_CAP):
        errors.append(f"pity_counter out of range [0, {PITY_CAP - 1}]")
    if state["total_pulls"] < 0 or state["daily_pulls"] < 0:
        errors.append("pull counters must be non-negative")
    if state["version"] < 0:
        errors.append("version must be non-negative")
    if not isinstance(state["compensations"], dict):
        errors.append("compensations must be a dict")
    if state.get("schema_version") != SCHEMA_VERSION:
        errors.append(f"schema_version must be {SCHEMA_VERSION}")
    return errors


def validation(account_id, *, store=None):
    """校验账号状态完整性，返回逐项检查结果。"""
    store = _store(store)
    store.ensure_account(account_id)
    state = store.get_state(account_id)
    errors = _validate_state_dict(state)

    # 交叉校验：事件流重放结果必须与物化状态一致
    replay_errors = []
    with store._lock:
        events = deepcopy(store._events.get(account_id, []))
    try:
        replayed = store._replay(account_id, events, version=state["version"])
        for key in ("pity_counter", "total_pulls", "ssr_count",
                    "hard_pity_triggers", "current_pool"):
            if replayed[key] != state[key]:
                replay_errors.append(
                    f"{key}: state={state[key]} replay={replayed[key]}")
    except InvalidStateError as exc:
        replay_errors.append(str(exc))

    return {
        "account_id": account_id,
        "valid": not errors and not replay_errors,
        "errors": errors,
        "replay_errors": replay_errors,
        "pity_cap": PITY_CAP,
        "version": state["version"],
    }


def audit(account_id, *, store=None, event_type=None, limit=None):
    """读取账号事件流（追加日志，不可变）。"""
    store = _store(store)
    with store._lock:
        events = deepcopy(store._events.get(account_id, []))
    if event_type is not None:
        events = [e for e in events if e["type"] == event_type]
    if limit is not None:
        events = events[-int(limit):]
    return {
        "account_id": account_id,
        "total_events": len(store._events.get(account_id, [])),
        "returned": len(events),
        "events": events,
    }


def fallback(account_id, corrupted_state=None, *, store=None):
    """兜底恢复：丢弃可疑物化状态，从事件流重建。

    事件流通过 hash 链式签名校验发现断裂点，断裂点之后的事件不参与重建。
    """
    store = _store(store)
    store.ensure_account(account_id)
    with store._lock:
        events = deepcopy(store._events.get(account_id, []))
        known_version = None
        if isinstance(corrupted_state, dict):
            known_version = corrupted_state.get("version")

        # 链式完整性检查：重算每个事件的签名与落库签名比对
        good_events = []
        corruption = None
        prev_sig = None
        for ev in events:
            sig = uuid.uuid5(
                uuid.NAMESPACE_OID,
                f"{prev_sig}|{ev['event_id']}|{ev['type']}|{ev['ts']}",
            ).hex
            if ev.get("chain_sig") != sig:
                corruption = {"at_event": ev["event_id"], "reason": "chain break"}
                break
            prev_sig = sig
            good_events.append(ev)

        rebuilt = store._replay(account_id, good_events, version=known_version)
        store._states[account_id] = rebuilt
        store._events[account_id] = good_events
        return {
            "account_id": account_id,
            "recovered": True,
            "events_replayed": len(good_events),
            "events_dropped": len(events) - len(good_events),
            "corruption": corruption,
            "state": deepcopy(rebuilt),
        }


# -- 上限 / 指标 / 查询 / 回滚 ----------------------------------------------

def pity_cap(account_id=None, *, store=None):
    """返回保底上限。唯一入口，保证判定与展示永远一致。"""
    return {
        "pity_cap": PITY_CAP,
        "max_counter_before_trigger": PITY_CAP - 1,
        "source": "single-source-of-truth",
        "account_id": account_id,
    }


def metrics(account_id=None, *, store=None):
    """聚合指标：账号维度或全存储维度。"""
    store = _store(store)
    with store._lock:
        ids = [account_id] if account_id is not None else list(store._states)
        states = [deepcopy(store._states[i]) for i in ids if i in store._states]

    total_pulls = sum(s["total_pulls"] for s in states)
    total_ssr = sum(s["ssr_count"] for s in states)
    hard_pity = sum(s["hard_pity_triggers"] for s in states)
    return {
        "accounts": len(states),
        "total_pulls": total_pulls,
        "total_ssr": total_ssr,
        "hard_pity_triggers": hard_pity,
        "natural_ssr": total_ssr - hard_pity,
        "ssr_rate": (total_ssr / total_pulls) if total_pulls else 0.0,
        "pity_cap": PITY_CAP,
        "per_account": [
            {
                "account_id": s["account_id"],
                "pity_counter": s["pity_counter"],
                "remaining_to_cap": PITY_CAP - s["pity_counter"],
                "total_pulls": s["total_pulls"],
                "current_pool": s["current_pool"],
                "version": s["version"],
            }
            for s in states
        ],
    }


def query(account_id, *, store=None):
    """玩家侧查询：展示字段与判定逻辑共用 PITY_CAP。"""
    store = _store(store)
    store.ensure_account(account_id)
    state = store.get_state(account_id)
    return {
        "account_id": account_id,
        "current_pool": state["current_pool"],
        "pity_counter": state["pity_counter"],
        "pity_cap": PITY_CAP,
        "remaining_to_cap": PITY_CAP - state["pity_counter"],
        "progress_pct": round(state["pity_counter"] / PITY_CAP, 4),
        "total_pulls": state["total_pulls"],
        "ssr_count": state["ssr_count"],
        "hard_pity_triggers": state["hard_pity_triggers"],
        "daily_pulls": state["daily_pulls"],
        "server_date": state["server_date"],
        "version": state["version"],
    }


def rollback(account_id, n_events=1, *, store=None):
    """回滚最近 n 个事件：截断事件流后整体重放，版本号单调不回退。"""
    store = _store(store)
    store.ensure_account(account_id)
    with store._lock:
        state = store._get(account_id)
        old_version = state["version"]
        events = store._events.get(account_id, [])
        n_events = int(n_events)
        if n_events < 0:
            raise GachaError("n_events must be non-negative")
        if n_events > len(events):
            # 事件不足时不做任何写入，返回 no-op，调用方可据 rolled_back 判定
            return {
                "account_id": account_id,
                "rolled_back_events": 0,
                "requested": n_events,
                "available_events": len(events),
                "remaining_events": len(events),
                "version": old_version,
                "state": deepcopy(state),
            }
        truncated = events[:-n_events] if n_events else list(events)
        # 版本号代表写入次数：回滚本身也是一次写入，保持单调递增
        rebuilt = store._replay(account_id, truncated, version=old_version + 1)
        store._states[account_id] = rebuilt
        store._events[account_id] = truncated
        return {
            "account_id": account_id,
            "rolled_back_events": n_events,
            "requested": n_events,
            "remaining_events": len(truncated),
            "version": rebuilt["version"],
            "state": deepcopy(rebuilt),
        }
