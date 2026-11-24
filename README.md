# pairwise-gacha-pity-counter

## 领域

游戏、图形与多媒体

## 主题

抽卡保底计数

## 说明

跨池迁移、并发扣减与版本兼容。

初始状态下 `gameplay/gacha_pity_counter.py` 中所有函数抛出 `NotImplementedError`，测试全部失败。
修复实现后运行：

```bash
python -m pytest -q
```

期望全部通过。

## 修复点与实现对照

| 线上缺陷 | 修复方案 | 关键入口 |
| --- | --- | --- |
| 保底按池独立计数，切池即重置 | 计数挂账号全局持有，切池只改 `current_pool`，计数不迁移不清零 | `pool_migrate` / `query` |
| 单抽与十连并发各读旧计数、重复触发保底 | 账号级互斥锁串行化所有读改写；state 带单调 `version`，支持 `expected_version` 乐观锁，过期写入抛 `StaleVersionError`；十连是单事务单事件 | `counter_apply` / `ten_pull` / `concurrent_pull` |
| 零点重置按客户端时间错位 | 重置只按存储配置的服务器时区计算，客户端时区/时间仅入审计事件；永久保底计数不参与每日重置 | `timezone_reset` |
| 版本升级后旧计数格式不兼容 | v0（按池 dict，多池取 max 合并）/ v1（扁平 `pity` 字段改名）映射到当前 schema，迁移幂等，超上限值钳制并留痕 | `version_migrate` |
| 补偿邮件重复发放且回加保底 | `compensation_id` 去重，重复发放返回首次结果且零写入；补偿只进背包，任何路径都不修改 `pity_counter` | `mail_compensate` |
| 保底上限与展示不一致 | `PITY_CAP` 为唯一常量，触发判定、查询展示、剩余次数、进度百分比全部同源 | `pity_cap` / `query` / `_draw_one` |

配套能力：追加事件流（链式 hash 签名）作为事实来源，物化状态可随时重放；
`rollback` 截断事件重建且版本号单调不回退，`fallback` 从损坏状态/断裂事件链恢复，
`validation` 做字段校验加事件流重放交叉核对，`determinism` 提供可复现抽卡序列，
`audit` / `metrics` 提供审计与聚合指标。

回归测试见 `tests/test_fixes.py`（17 个用例，每个修复点至少一条），
原契约测试 `tests/test_gacha_pity_counter.py` 保持通过。
