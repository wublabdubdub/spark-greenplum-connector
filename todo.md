# 待办

最后更新：2026-04-03

## 进行中 / 未完成

- 用真实 Spark 作业验证“空判断前移 + executor 预热”后的前后耗时对比。
- 评估是否需要把 `write.batch.settle.ms`、executor 预热和判空位置补充到正式操作文档中。
- 为高分区写入补一条回归验证或探针场景，覆盖 task 分布到多 host 的情况。
- 考虑在写分区数远高于有效 segment/provider 能力时增加 warning 日志。
- 对照 `docs/xiaomi-2026-04-03-ymatrix同步复盘.md` 持续更新现场结论和验证结果。
- 确认业务侧提交脚本中已移除 `failed.partition.dump.*` 三个参数，避免传无效配置。

## 已完成

- 在仓库根目录建立项目记忆文件。
- 写入“每次修改都必须更新 `status.md` 和 `todo.md`”的约定。
- 改造写路径 batch 收敛逻辑，去掉固定启动等待。
- 修复 `GreenplumScan.scala` 的 Spark 3.4 API 编译兼容问题。
- 将 `project.md`、`skiils.md`、`status.md`、`todo.md` 四份项目文档改为中文。
- 补充 Xiaomi 同步复盘文档，并把示例/探针改成“先判空、后 repartition”的推荐写法。
- 删除 `failed.partition.dump.*` 对应的参数解析和失败 dump 功能实现。
