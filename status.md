# 状态记录

最后更新：2026-04-03

## 当前状态

- 仓库状态：持续开发中，工作区存在其他未提交改动。
- 主模块：`spark-greenplum-connector`
- 附属模块：`test-app`
- 项目记忆文件已放在仓库根目录统一维护。

## 变更日志

### 2026-04-01 第 1 次记录

摘要：

- 去掉了 `guessMaxParallelTasks` 中固定 30 秒等待。
- 新增 `write.batch.settle.ms`，默认值为 `300ms`。
- 写 batch 的冻结条件从“猜测 executor 数量”改为“短静默窗口收敛”。
- 放宽了写路径中 satellite task 的挂靠条件，使新 host 可以直接跟随已有 `gpfdist` provider，避免过早 abort。
- 新增项目记忆文件：`project.md`、`skiils.md`、`status.md`、`todo.md`。

涉及文件：

- `spark-greenplum-connector/src/main/scala/org/apache/spark/sql/itsumma/gpconnector/SparkSchemaUtil.scala`
- `spark-greenplum-connector/src/main/scala/org/apache/spark/sql/itsumma/gpconnector/GPOptionsFactory.scala`
- `spark-greenplum-connector/src/main/scala/com/itsumma/gpconnector/rmi/RMIMaster.scala`
- `project.md`
- `skiils.md`
- `status.md`
- `todo.md`

验证：

- 执行了 `mvn -pl spark-greenplum-connector -DskipTests compile`
- 当时结果：被 `spark-greenplum-connector/src/main/scala/com/itsumma/gpconnector/reader/GreenplumScan.scala` 中的现存编译问题阻塞
- 错误：`object Distribution is not a member of package org.apache.spark.sql.connector.read.partitioning`

备注：

- 当时的编译失败并非由上述三处写路径改动直接引入。

### 2026-04-01 第 2 次记录

摘要：

- 修复了 `GreenplumScan.scala` 中与 Spark 3.4 API 的兼容性问题。
- 将四份项目文档全部改为中文。

涉及文件：

- `spark-greenplum-connector/src/main/scala/com/itsumma/gpconnector/reader/GreenplumScan.scala`
- `project.md`
- `skiils.md`
- `status.md`
- `todo.md`

验证：

- 再次执行 `mvn -pl spark-greenplum-connector -DskipTests compile`
- 结果：`BUILD SUCCESS`

备注：

- 目前主模块已恢复可编译状态。
- 后续所有修改必须继续按本文件追加记录。

### 2026-04-03 第 1 次记录

摘要：

- 增加了 Xiaomi 同步专题复盘文档，集中记录故障判断、性能拆解、优化决策和后续验证计划。
- 将两个 Iceberg -> YMatrix 示例脚本改为“先轻量判空，再按分布键 repartition 写入”的推荐模式。
- 将 `YMatrixWriteStageProbe` 探针改为先执行 `limit(1)` 判空，再构造写入 DataFrame，避免把空判断放在 `repartition` 之后。
- 将项目记忆更新到 2026-04-03，补充失败 dump 和写前性能优化方向。

涉及文件：

- `examples/iceberg-to-ymatrix-bulk-load.scala`
- `examples/iceberg-to-ymatrix-incremental-sync.scala`
- `test-app/src/main/java/com/xiaomi/YMatrixWriteStageProbe.java`
- `docs/xiaomi-2026-04-03-ymatrix同步复盘.md`
- `project.md`
- `status.md`
- `todo.md`

验证：

- 执行了 `mvn -pl test-app -DskipTests compile`
- 结果：`BUILD SUCCESS`
- 执行了 `mvn -pl spark-greenplum-connector -DskipTests compile`
- 结果：`BUILD SUCCESS`

备注：

- 本次主要是示例、探针和复盘文档更新，没有改动核心写链路默认行为。

### 2026-04-03 第 2 次记录

摘要：

- 按要求彻底删除了 `failed.partition.dump.*` 相关调试功能。
- 删除了写分区失败 dump 和 gpfdist 流级 dump 两套运行时代码。
- 移除了对应参数解析和 JDBC 参数过滤逻辑。
- 同步清理了复盘文档中对该功能的描述。

涉及文件：

- `spark-greenplum-connector/src/main/scala/org/apache/spark/sql/itsumma/gpconnector/GPOptionsFactory.scala`
- `spark-greenplum-connector/src/main/scala/com/itsumma/gpconnector/writer/GreenplumDataWriter.scala`
- `spark-greenplum-connector/src/main/scala/com/itsumma/gpconnector/rmi/RMISlave.scala`
- `spark-greenplum-connector/src/main/scala/com/itsumma/gpconnector/gpfdist/WebServer.scala`
- `spark-greenplum-connector/src/main/scala/com/itsumma/gpconnector/writer/FailedPartitionDebugBuffer.scala`
- `spark-greenplum-connector/src/main/scala/com/itsumma/gpconnector/gpfdist/GpfdistFailureDump.scala`
- `docs/xiaomi-2026-04-03-ymatrix同步复盘.md`
- `status.md`
- `todo.md`

验证：

- 执行了 `mvn -pl spark-greenplum-connector -DskipTests compile`
- 结果：`BUILD SUCCESS`
- 执行了 `mvn -pl spark-greenplum-connector -am -DskipTests package`
- 结果：`BUILD SUCCESS`

备注：

- 删除后，`failed.partition.dump.enabled`、`failed.partition.dump.max.lines`、`failed.partition.dump.dir` 不再生效，也不应继续传参。
