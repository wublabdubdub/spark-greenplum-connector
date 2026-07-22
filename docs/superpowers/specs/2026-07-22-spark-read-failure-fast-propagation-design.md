# Spark 读取异常快速终止与原始错误传播设计

## 背景

当前 YMatrix Spark connector 在读取不存在的 MDB 表时，数据库错误发生在 `GreenplumScan.SqlThread` 后台线程。该线程记录错误后退出，但没有把失败状态和原始错误通知给正在等待 gpfdist 数据的 Spark reader task。reader 只能等待 `server.timeout` 到期，因此会出现数据库已经报错、Spark Stage 仍长时间运行的现象。

同时，`SparkSchemaUtil.getGreenplumTableSchema()` 在普通 `dbtable` 不存在时返回空 schema，使 `CREATE TEMPORARY VIEW` 成功，错误被延迟到真正执行 action 时才暴露。

## 目标

1. 普通 `dbtable` 不存在时，在临时视图绑定阶段同步失败，不创建 Spark Stage。
2. 读取过程中发生数据库、外部表或协调线程异常时，立即终止所有 reader task，不等待 `network.timeout` 或 `server.timeout`。
3. 将原始 MDB 错误文本传播到 Spark task 和最终的 Spark action 异常中，使调用方外层 `catch` 能识别 `relation ... does not exist` 等根因。
4. 保持正常读取、流式读取、写入路径和已有 CN2 地址映射行为不变。

## 非目标

- 不修改 Spark 自身的 task retry 策略。
- 不改变 `network.timeout`、`server.timeout` 的含义或默认值。
- 不增加 MDB 在线业务验证；本次只进行自动化回归测试、编译和 JAR 内容检查。
- 不重构与异常传播无关的 gpfdist、RMI 或读写协议。

## 方案

### 1. 绑定阶段失败

当 `dbtable` 是普通表名且表不存在时，schema 推断不再返回空 `StructType`，而是抛出包含目标对象名的 `SQLException`。查询形式的 `dbtable` 继续由 JDBC prepare 阶段返回原始数据库错误。

只有确实允许空 schema 的自定义传输场景保留现有占位 schema 行为，避免破坏 `sqltransfer` 的既有能力。

### 2. 后台异常统一收口

`GreenplumScan.SqlThread.run()` 增加覆盖整个处理循环的异常捕获。捕获到 fatal error 后按以下顺序执行：

1. 保存第一个原始异常，避免后续清理异常覆盖根因。
2. 设置 `aborted=true` 和 `processing=false`。
3. 在 `RMIMaster` 仍可用时调用 `failJob()`。
4. 向所有已注册 reader 广播 `sqlTransferAbort` 和标准化后的原始错误文本。
5. 最后再执行现有 RMI、JDBC 和临时对象清理。

所有建外部表、执行 MDB SQL、批次提交和协调等待产生的异常都走同一条路径。

### 3. RMI 传递失败原因

在 `PartitionControlBlock` 增加可空的失败原因字段。driver 发送 `sqlTransferAbort` 时携带原始错误；executor 收到后将其保存在本地失败状态中并设置 `jobAbort`。

driver 和 executor 必须使用同一个新 JAR。该字段只用于当前 Spark 应用内部的 RMI 通信，不修改 MDB 协议、gpfdist URL 或数据格式。

### 4. Reader 快速失败

`RMISlave.read()` 在检测到 `jobAbort` 后立即抛出包含原始失败原因的异常，不再把主动中止误报为 `Time limit elapsed`。正常超时仍保留原有超时异常。

这样 Spark task 会立即失败，Spark Stage 随即结束，调用 `.show()`、`.count()` 等 action 的线程得到异常并进入外层 `catch`。

## 错误优先级与清理

- 始终保留第一次 fatal error 作为根因。
- 清理外部表、关闭 RMI 或回滚 JDBC 时发生的异常只记录日志，不覆盖根因。
- `failJob()` 必须早于 `rmiMaster.stop()`。
- abort 广播失败时仍继续本地清理，并依赖现有连接关闭和 Spark task 失败机制退出。
- 正常结束路径不设置失败原因，也不改变原有 commit 行为。

## 自动化测试

测试覆盖以下行为：

1. 普通表不存在时 schema 推断抛错，不返回空 schema。
2. abort 广播携带原始 MDB 错误文本。
3. reader 收到 abort 后立即抛错，错误文本包含原始原因，而不是等待超时。
4. 正常完成信号仍返回 EOF，不被误判为 abort。
5. 现有 CN2 `ServerPublishMappingTest` 继续通过。

不执行远端 MDB 在线读写验证。

## 构建与交付

代码先在本地修改。之后同步必要源码到 `172.16.100.143:/root/spark-greenplum-connector`，在远端按项目既有 Maven 配置编译 Scala 2.12 / Spark 3.4.1 connector，并生成包含运行依赖的 shaded JAR。

最终 JAR 回传到当前本地项目目录，文件名格式为：

```text
spark-ymatrix-connector_2.12-3.1_YYYYMMDD_HHMMSS.jar
```

交付前检查 JAR 可读取、关键 connector/RMI 类存在、服务注册文件存在，且构建使用的源码包含本次失败传播修复。
