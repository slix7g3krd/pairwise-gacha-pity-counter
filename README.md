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
