# 仓库技能

最后更新：2026-04-01

## 技能：排查写入耗时

- 先看 driver 汇总日志中的 `processingMs`、`initMs`、`settleMs`、`commitMs`、`webTransferMs`。
- 对比 `RMIMaster` 启动时刻和第一批写任务真正开始的时间差。
- 在判断数据库慢之前，优先检查 `GreenplumBatchWrite.scala` 和 `RMIMaster.scala`。

## 技能：排查高 `repartition` 失败

- 不只看最终的 `Connection refused`，必须追到第一条真正的失败原因。
- 重点检查 `RMIMaster.handlerAsks`、`RMIMaster.connect` 以及 satellite/provider 的分配逻辑。
- 分清楚到底是 segment 分配失败、provider 启动失败，还是 driver abort 后的晚到重试。

## 技能：安全修改仓库

- 开始动手前先看 `git status --short`，因为本仓库经常是脏工作区。
- 改动范围尽量收敛到当前任务。
- 避免触碰用户已修改但与当前任务无关的文件。

## 技能：验证

- 优先使用有针对性的编译或测试命令，不做大范围清理。
- 如果验证失败是由仓库中已有的无关问题导致，必须写进 `status.md`。

## 技能：维护项目记忆

- 每次修改都必须在 `status.md` 中补充或更新带日期的记录。
- 每个新增待办、风险或后续动作都必须反映到 `todo.md`。
- `project.md`、`status.md`、`todo.md` 和本文件共同构成项目记忆集合。
