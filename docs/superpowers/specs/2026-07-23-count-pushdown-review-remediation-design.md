# Spark COUNT 下推审查问题修复设计

## 目标

修复 2026-07-23 全面代码审查发现的四项风险，同时保持普通读取、
过滤下推、流式读取和写入路径的现有语义不变。

成功标准：

1. COUNT 下推不要求 Spark Executor 直连 MDB。
2. Spark 在调用 `pushAggregation()` 之前调用
   `supportCompletePushDown()` 时，连接器仍能正确声明完整下推。
3. `RMISlave` 在 registry lookup 或 check-in 失败时立即解除 RMI export。
4. 隐藏分布键投影每行只创建一个 `InternalRow`。
5. 数据库、RMI 和解析失败继续显式抛出，不允许回退为正常 EOF 或 `0`。

## 方案选择

### 采用：Driver 计算 COUNT，Executor 返回字面值

`GreenplumCountScan.planInputPartitions()` 在 Driver 上通过现有
`GreenplumRowSet.getGpClient` 借用 JDBC 连接，执行一次
`SELECT COUNT(*)::bigint`。结果封装进唯一的
`GreenplumCountInputPartition(countValue)`。

Executor 侧的 reader 不再包含 `GPOptionsFactory`、JDBC URL、认证信息或
数据库连接，只把分区携带的 Long 返回一次。

该方案保留普通读取既有的网络边界：只有 Driver 需要直连 MDB。

### 不采用：Executor JDBC 加预检

预检只能提前暴露问题，不能消除 Executor 直连 MDB、获取 keytab 或密码文件
的新增要求。

### 不采用：gpfdist 传输后由 Spark 计数

这会重新引入千万行传输和外部表创建，违背 COUNT 下推的性能目标。

## 组件设计

### 1. DriverCountQuery

在 `GreenplumCountScan.scala` 中保留 `CountSql`，新增只负责执行 SQL 的
Driver 侧查询单元：

- 从 `GreenplumRowSet.getGpClient` 借用连接；
- 调用 `GPClient.checkDbObjSearchPath` 获得默认 schema；
- 生成并执行 COUNT SQL；
- 校验结果必须正好可以读取一行且第一列非 NULL；
- 在 finally 中关闭 `ResultSet`、`PreparedStatement` 和借用的连接；
- 不关闭 `GreenplumRowSet` 共享的 `GPClient` 连接池。

`GreenplumCountScan` 使用线程安全的 lazy 值，保证 Spark 多次调用
`planInputPartitions()` 时数据库 COUNT 只执行一次。

### 2. 完整聚合下推判断

`supportCompletePushDown()` 必须只依赖当前传入的 `Aggregation`、
未下推过滤器状态和 `sqlTransfer`，不能依赖 `pushAggregation()` 写入的
`countPushdown` 状态。

新增回归顺序：

1. 先调用 `supportCompletePushDown()`，预期 true；
2. 再调用 `pushAggregation()`，预期 true；
3. `build()` 返回 `GreenplumCountScan`。

不支持的过滤器、分组聚合、非 COUNT 聚合和自定义 `sqlTransfer` 继续返回
false。

### 3. RMI 启动失败清理

新增小型 `RmiObjectLifecycle` 工具，负责强制调用
`UnicastRemoteObject.unexportObject(remote, true)` 并把“已经解除”视为幂等。

`RMISlave` 以下两个构造失败分支必须先调用该工具，再抛出原始失败：

- registry lookup 失败；
- coordinator check-in 失败。

错误消息根据 `readOrWrite` 正确显示 read 或 write。

正常 `stop()` 路径保持现有行为。

### 4. 单行对象投影

`ReadRowProjector` 不再接收已经构造的 transfer `InternalRow`。
它预先计算 output 字段在 transfer schema 中的索引，然后把原始文本字段数组
连同索引交给 `SparkSchemaUtil.textToInternalRow` 的索引重载。

索引重载直接构造 output schema 对应的 `SpecificInternalRow`：

- 校验 transfer 字段数量；
- 仅解析 output 所需字段；
- 隐藏分布键不再先解析成 transfer row；
- 空 output schema 仍为每个输入文本行返回一个零字段 row，供 Spark 自己计数。

普通完整 schema 读取走 identity 索引，行为保持不变。

## 错误处理

- Driver COUNT 的 SQL、认证和连接错误从
  `planInputPartitions()` 原样抛给 Spark Driver。
- COUNT 返回空结果或 NULL 时抛 `IllegalStateException`。
- RMI 构造失败保留原异常为 cause，清理异常作为 suppressed exception。
- 文本字段数或投影索引不一致时抛 `SQLException`/`IllegalArgumentException`，
  不跳过数据。

## 测试设计

### COUNT

- Spark 实际调用顺序：complete-before-push。
- Driver COUNT 正常返回 Long。
- SQL 异常原样传播。
- NULL 和空结果失败。
- `planInputPartitions()` 多次调用只执行一次 loader。
- Executor reader 不包含 JDBC 依赖并且只返回一次。

### RMI

- 导出测试 remote 后调用启动失败清理；
- 再次 unexport 证明对象已解除；
- 重复清理不抛错。

### 投影

- 单分布键、多分布键、quoted key 和 random fallback。
- 输出列重排。
- Decimal/String/Long 与 NULL。
- 空 output schema。
- 验证生产路径直接从文本字段生成 output row。

### 回归

- 编译全部主源码和测试源码。
- 执行现有及新增的纯边界测试对象。
- 在 143 重新打包。
- 检查最终 JAR 包含新增/修改类并计算 SHA-256。
- 按用户要求不执行数据库实际联调。

## 非目标

- 不部署到 `/opt/spark/jars`。
- 不修改现有 MDB 表或数据。
- 不扩展到 SUM、AVG、MIN、MAX 或 GROUP BY 下推。
- 不重构无关的写入路径和流式 offset 逻辑。
