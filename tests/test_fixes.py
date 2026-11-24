"""六个修复点的回归测试：每个测试对应一类线上缺陷。"""

from datetime import datetime, timedelta, timezone

import pytest

import gameplay.gacha_pity_counter as mod
from gameplay.gacha_pity_counter import (
    BASE_SSR_RATE,
    PITY_CAP,
    GachaStore,
    StaleVersionError,
    counter_apply,
    concurrent_pull,
    determinism,
    fallback,
    mail_compensate,
    metrics,
    pity_cap,
    pool_migrate,
    query,
    rollback,
    ten_pull,
    timezone_reset,
    validation,
    version_migrate,
    audit,
)


def seed_without_natural_ssr(n):
    """找一个前 n 抽都不会自然出 SSR 的种子，保证用例只验证保底机制。"""
    for seed in range(10000):
        seq = determinism(seed, n)["sequence"]
        if all(u >= BASE_SSR_RATE for u in seq):
            return seed
    raise AssertionError("no safe seed found")


# -- 修复点 1：计数按账号全局持有，跨池不重置 -------------------------------

def test_global_counter_survives_pool_switch():
    store = GachaStore()
    seed = seed_without_natural_ssr(10)

    r1 = counter_apply(777, 5, store=store, seed=seed)
    counter_after_pull = r1["pity_counter"]
    assert counter_after_pull == 5

    m1 = pool_migrate(777, to_pool="pool_b", store=store)
    m2 = pool_migrate(777, to_pool="pool_c", store=store)

    assert m1["counter_migrated"] is True
    assert m1["pity_counter"] == counter_after_pull
    assert m2["pity_counter"] == counter_after_pull

    state = query(777, store=store)
    assert state["current_pool"] == "pool_c"
    assert state["pity_counter"] == counter_after_pull  # 切池没有清零


# -- 修复点 2：抽卡串行化 + 版本号，并发不重复触发保底 ----------------------

def test_concurrent_pulls_trigger_hard_pity_exactly_once():
    store = GachaStore()
    seed = seed_without_natural_ssr(PITY_CAP)
    # 先把账号计数推进到保底前一抽
    counter_apply(42, PITY_CAP - 1, store=store, seed=seed)
    assert query(42, store=store)["pity_counter"] == PITY_CAP - 1

    result = concurrent_pull(42, n=20, store=store)

    assert result["invariants_ok"] is True
    assert result["committed"] == 20
    assert result["hard_pity_triggers"] == 1          # 不会重复触发保底
    final = query(42, store=store)
    assert final["pity_counter"] < PITY_CAP            # 触发后已归零重新累计
    assert final["total_pulls"] == (PITY_CAP - 1) + 20
    # 每次提交版本号都递增，无丢失更新
    assert final["version"] == 1 + 20


def test_stale_version_write_is_rejected():
    store = GachaStore()
    counter_apply(9, 1, store=store, seed=1)
    stale = query(9, store=store)["version"]
    counter_apply(9, 1, store=store, seed=2)  # 版本已前进

    with pytest.raises(StaleVersionError):
        counter_apply(9, 1, store=store, expected_version=stale, seed=3)

    # 过期写入完全没有落地
    state = query(9, store=store)
    assert state["version"] == 2


def test_ten_pull_is_single_atomic_transaction():
    store = GachaStore()
    result = ten_pull(5, store=store, seed=seed_without_natural_ssr(10))
    assert result["count"] == 10
    assert query(5, store=store)["total_pulls"] == 10
    assert query(5, store=store)["version"] == 1      # 整批只递增一次
    pull_events = audit(5, store=store, event_type="pull")["events"]
    assert len(pull_events) == 1                       # 整批只有一个事件


# -- 修复点 3：零点重置按服务器时区，客户端时间只作审计 ---------------------

def test_daily_reset_ignores_client_timezone():
    store = GachaStore(server_tz="UTC")
    # 服务器时间 1 月 1 日 23:30 UTC
    counter_apply(11, 1, store=store,
                  now=datetime(2026, 1, 1, 23, 30), seed=1)
    pity_before = query(11, store=store)["pity_counter"]

    # 客户端在东九区，自称已经是 1 月 2 日早上
    client_now = datetime(2026, 1, 2, 8, 0,
                          tzinfo=timezone(timedelta(hours=9)))
    r = timezone_reset(
        11, store=store, now=datetime(2026, 1, 1, 23, 30),
        client_tz="Asia/Tokyo", client_now=client_now)

    assert r["reset_performed"] is False               # 服务器没过零点就不重置
    assert r["pity_counter"] == pity_before            # 永久保底任何时候都不动

    # 服务器时间跨入 1 月 2 日
    r2 = timezone_reset(11, store=store,
                        now=datetime(2026, 1, 2, 0, 5))
    assert r2["reset_performed"] is True
    assert r2["server_date"] == "2026-01-02"
    assert r2["pity_counter"] == pity_before


def test_daily_reset_uses_configured_server_timezone():
    # 服务器在 UTC+8：UTC 1 月 1 日 16:30 对服务器已是 1 月 2 日
    store = GachaStore(server_tz=timezone(timedelta(hours=8)))
    counter_apply(12, 1, store=store,
                  now=datetime(2026, 1, 1, 23, 0), seed=1)  # 服务器本地 1/1
    r = timezone_reset(
        12, store=store,
        now=datetime(2026, 1, 1, 16, 30, tzinfo=timezone.utc))
    assert r["reset_performed"] is True
    assert r["server_date"] == "2026-01-02"


# -- 修复点 4：旧 schema 迁移映射，且幂等 -----------------------------------

def test_migrate_v0_per_pool_counters():
    raw = {
        "schema_version": 0,
        "last_pool": "pool_b",
        "pools": {
            "pool_a": {"pity": 10},
            "pool_b": {"pity": 25},
        },
        "total_pulls": 35,
    }
    out = version_migrate({**raw, "account_id": "acc1"})
    state = out["state"]
    assert state["schema_version"] == 2
    assert state["pity_counter"] == 25                 # 多池取最大值
    assert state["current_pool"] == "pool_b"
    assert state["account_id"] == "acc1"

    # 再迁一次是 no-op
    again = version_migrate(state)
    assert again["notes"][0]["action"] == "noop"
    assert again["state"]["pity_counter"] == 25


def test_migrate_v1_flat_counter():
    out = version_migrate({"schema_version": 1, "pity": 7, "pool": "p_x",
                           "account_id": "acc2"})
    state = out["state"]
    assert state["pity_counter"] == 7
    assert state["current_pool"] == "p_x"


def test_migrate_clamps_legacy_counter_above_cap():
    out = version_migrate({"pity": PITY_CAP + 50, "account_id": "acc3"})
    assert out["state"]["pity_counter"] == PITY_CAP - 1
    assert any("clamp" in n["action"] for n in out["notes"])


def test_migrate_into_store_is_idempotent():
    store = GachaStore()
    # 账号 id + legacy：迁移后落库
    r1 = version_migrate("acc4", {"pity": 3}, store=store)
    assert r1["migrated"] is True
    assert query("acc4", store=store)["pity_counter"] == 3

    # 再对已存在账号迁移应返回 already-current，不重复生效
    r2 = version_migrate("acc4", store=store)
    assert r2["migrated"] is False
    assert query("acc4", store=store)["pity_counter"] == 3


# -- 修复点 5：补偿幂等，且绝不回改保底 -------------------------------------

def test_compensation_idempotent_and_never_touches_pity():
    store = GachaStore()
    counter_apply(21, 3, store=store,
                  seed=seed_without_natural_ssr(5))
    pity_before = query(21, store=store)["pity_counter"]
    version_before = query(21, store=store)["version"]

    first = mail_compensate(21, "mail-001", {"gem": 800}, store=store)
    assert first["delivered"] is True and first["duplicate"] is False

    second = mail_compensate(21, "mail-001", {"gem": 800}, store=store)
    assert second["delivered"] is False and second["duplicate"] is True

    state = query(21, store=store)
    assert state["pity_counter"] == pity_before        # 补偿不动保底
    assert state["version"] == version_before + 1      # 重复发放不产生写入
    inv = store.get_state(21)["inventory"]
    assert inv["gem"] == 800                           # 道具只发一次


# -- 修复点 6：保底上限与展示一致 -------------------------------------------

def test_pity_cap_single_source_of_truth():
    store = GachaStore()
    seed = seed_without_natural_ssr(PITY_CAP)
    counter_apply(31, PITY_CAP - 1, store=store, seed=seed)

    q = query(31, store=store)
    cap = pity_cap(31, store=store)
    assert q["pity_cap"] == PITY_CAP == cap["pity_cap"]
    assert q["remaining_to_cap"] == 1
    assert q["progress_pct"] == round((PITY_CAP - 1) / PITY_CAP, 4)

    last = counter_apply(31, 1, store=store, seed=seed)
    assert last["hard_pity"] is True                   # 第 90 抽必触发
    assert last["pity_counter"] == 0
    assert last["remaining_to_cap"] == PITY_CAP
    assert query(31, store=store)["pity_counter"] == 0


# -- 确定性、校验、回滚、兜底、指标等配套能力 -------------------------------

def test_deterministic_sequence_reproducible():
    a = determinism(12345, 20)
    b = determinism(12345, 20)
    assert a["sequence"] == b["sequence"] and a["reproducible"] is True

    s1, s2 = GachaStore(), GachaStore()
    r1 = counter_apply("a", 10, store=s1, seed=999)
    r2 = counter_apply("b", 10, store=s2, seed=999)
    assert r1["pulls"] == r2["pulls"]


def test_validation_passes_and_detects_drift():
    store = GachaStore()
    counter_apply(33, 5, store=store, seed=1)
    assert validation(33, store=store)["valid"] is True

    # 人为破坏物化状态，校验应能发现
    store._states[33]["pity_counter"] = PITY_CAP + 999
    assert validation(33, store=store)["valid"] is False


def test_rollback_rebuilds_state_from_events():
    store = GachaStore()
    counter_apply(44, 3, store=store, seed=1)
    ten_pull(44, store=store, seed=2)
    assert query(44, store=store)["total_pulls"] == 13

    rb = rollback(44, 1, store=store)
    assert rb["rolled_back_events"] == 1
    assert query(44, store=store)["total_pulls"] == 3
    # 回滚也是一次写入，版本号单调不回退
    assert rb["version"] == 3


def test_fallback_recovers_from_corrupted_state_and_broken_chain():
    store = GachaStore()
    counter_apply(55, 4, store=store, seed=1)
    counter_apply(55, 2, store=store, seed=2)

    # 1) 物化状态损坏：从完整事件流重建
    recovered = fallback(55, {"version": 999, "pity_counter": -1},
                         store=store)
    assert recovered["corruption"] is None
    assert query(55, store=store)["total_pulls"] == 6

    # 2) 事件链被篡改：断裂点之后的事件丢弃
    store._events[55][-1]["type"] = "tampered"
    recovered2 = fallback(55, store=store)
    assert recovered2["corruption"] is not None
    assert recovered2["events_dropped"] == 1
    assert query(55, store=store)["total_pulls"] == 4


def test_metrics_reports_consistent_totals():
    store = GachaStore()
    counter_apply(66, 10, store=store, seed=1)
    m = metrics(store=store)
    assert m["accounts"] == 1
    assert m["total_pulls"] == 10
    assert m["pity_cap"] == PITY_CAP
    assert 0.0 <= m["ssr_rate"] <= 1.0
