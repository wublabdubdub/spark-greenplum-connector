# Spark 读取列裁剪、COUNT 下推与失败传播改造设计

## 背景

YMatrix Spark connector 读取按 `order_id` 分布的表时，会把目标表分布策略原样用于读取中转的 writable external table。Spark 执行 `count()` 等操作时会裁剪不参与结果计算的字段；当裁剪后的外部表 schema 不再包含 `order_id`，连接器仍生成 `DISTRIBUTED BY (order_id)`，MDB 因分布键不存在而拒绝创建外部表。

当前读侧还存在第二个正确性问题：driver 后台 SQL 线程失败并关闭 RMI Master 后，重试 reader 连接失败可能被解释成“空分区”。因此数据库已经报错时，Spark action 仍可能返回 `0`。

现场目标表约有 1000 万行。仅用随机分布绕过 DDL 错误可能引入 MDB Segment 间的数据重分布；仅给每行补传 `order_id` 又会让 `count()` 无意义地传输 1000 万行。因此改造需要分别处理标量聚合和普通明细读取。

## 目标

1. `df.count()` 且过滤条件均可下推时，由 MDB 完整执行 `COUNT(*)`，Spark 只读取一行结果。
2. 普通明细读取经过列裁剪后，如果缺少目标表分布键，连接器仅在内部传输 schema 中补充分布键。
3. 中转 writable external table 保持目标表的合法分布策略；内部增加的字段不出现在 Spark 可见结果中。
4. MDB SQL、外部表、RMI 启动或 check-in 失败时，Spark action 必须失败，不能返回空结果。
5. 第一条数据库根因应尽量保留到 executor task 和最终 Spark 异常中。
6. 正常零行查询、无需工作的分区和正常 EOF 继续返回空结果，不被误判为失败。

## 非目标

- 本次不实现通用的 `SUM`、`AVG`、`MIN`、`MAX` 或分组聚合下推。
- 本次不修改目标业务表的 DDL、分布键或存储方式。
- 本次不改变写入链路和 `server.publish.mapping` 行为。
- 本次不把所有读取中转表统一改为 `DISTRIBUTED RANDOMLY`。
- 本次不改变 Spark 自身的 task retry 配置。

## 总体架构

读取计划分为两条互斥路径：

### 路径 A：完整 `COUNT(*)` 下推

`GreenplumScanBuilder` 实现 Spark 3.4.1 的 `SupportsPushDownAggregates`，只接受以下聚合：

- 没有 `GROUP BY`；
- 聚合表达式只有一个 `CountStar`；
- 所有 Spark filter 都已经成功下推到 connector；
- 数据源是普通表或 connector 已能安全包裹的查询。

接受后生成一条 MDB 标量 SQL：

```sql
SELECT count(*)::bigint AS connector_count
FROM <source>
WHERE <pushed_filters>
```

该路径使用独立的单分区 JDBC scalar scan，不启动 gpfdist、writable external table 或 RMI 协调器。executor 只执行一次 SQL并返回一个 `LongType` 值。`supportCompletePushDown()` 返回 `true`，Spark 不再对结果进行二次计数。

如果聚合形式或 filter 不满足上述条件，`pushAggregation()` 返回 `false`，自动回到普通明细读取路径，保证语义正确。

### 路径 B：普通明细读取

连接器同时维护三套信息：

- `sourceSchema`：绑定 MDB 表时取得的完整 schema；
- `outputSchema`：Spark 列裁剪后真正要求返回的 schema；
- `transferSchema`：外部表和 gpfdist 实际传输使用的内部 schema。

`transferSchema` 初始等于 `outputSchema`。当目标表是按列分布，并且 `outputSchema` 缺少一个或多个分布键时，从 `sourceSchema` 中按标识符规则补齐缺失分布键。补充字段追加在末尾，不改变 Spark 要求字段的顺序。

外部表 DDL、`INSERT ... SELECT` 和文本解析使用 `transferSchema`。返回 Spark 前，reader 按字段索引把解析后的 `InternalRow` 投影回 `outputSchema`。如果 `outputSchema` 为空，仍然为每条输入记录返回一个零字段行，从而保持 `count()` 等 Spark 上层语义。

如果某个分布键无法在 `sourceSchema` 中解析，连接器不生成非法 DDL，而是记录 warning 并对该中转表使用 `DISTRIBUTED RANDOMLY`。这是异常元数据场景的最后兜底，不是正常列裁剪路径的默认行为。

## 标识符匹配

分布键匹配遵循 PostgreSQL 标识符规则：

- 未加双引号的列名按不区分大小写匹配；
- 加双引号的列名去掉外围双引号后按精确大小写匹配；
- 多列分布要求全部分布键都能解析；
- `DISTRIBUTED RANDOMLY` 和 `DISTRIBUTED REPLICATED` 没有列依赖，保持原样。

匹配和 schema 规划放在独立的纯函数组件中，以便不连接 MDB 就能回归测试。

## Reader 投影

`GreenplumInputPartitionReader` 同时接收 `transferSchema` 和 `outputSchema`：

1. 按 `transferSchema` 解析 gpfdist 文本行；
2. 预先计算 `outputSchema` 每个字段在 `transferSchema` 中的索引；
3. 每行仅按索引构造 Spark 可见的 `InternalRow`；
4. 隐藏分布键不进入 `readSchema()`，也不暴露给上层 DataFrame。

投影映射在 reader 初始化时计算一次，不在每行重复解析字段名。

## 失败传播

失败路径按以下规则处理：

1. `GreenplumScan` 继续使用 `sqlFailure` 保存第一条 driver SQL 失败。
2. planning、reader factory 创建和后续 batch 入口都先调用 `throwIfSqlFailed()`。
3. `RMIMaster.failJob()` 保留第一条标准化数据库错误并向已经注册的 reader 广播。
4. read reader 无法查找 RMI registry、无法取得 Master 或 check-in 抛错时必须抛异常，不再把 `server = null` 当成 EOF。
5. Master 成功响应但明确没有给分区分配工作时，reader 才能返回正常空分区。
6. `GreenplumInputPartitionReader.next()` 只有收到正常完成信号后才返回 EOF；未连接、协议中断和 abort 状态全部抛错。
7. 清理异常只记录日志，不覆盖第一条 MDB 根因。

这套规则保证“真实零行”和“读取失败”在协议状态上可区分。

## 性能预期

### COUNT 路径

MDB 扫描并聚合 1000 万行，但只向 Spark 返回一个 `bigint`。不会传输 1000 万条 gpfdist 明细，也不会为本次统计创建 writable external table。

### 明细路径

缺少 `order_id` 时，每条实际返回的明细会额外传输一个文本形式的 `bigint`，然后在 reader 内丢弃。假设平均 10 位数字加一个分隔符，1000 万行约增加 110 MB 原始传输量。该代价只发生在确实要读取明细且 Spark 裁掉了分布键的场景，用于换取保持原表分布并避免潜在的大规模 Segment Motion。

## 兼容性

- 未触发聚合下推的现有 DataFrame 和 Spark SQL 读取继续走原 gpfdist 路径。
- 全字段读取已经包含分布键时，`transferSchema == outputSchema`，不增加字段和投影成本。
- 不可下推 filter 存在时拒绝完整 COUNT 下推，由 Spark 继续计算，避免过滤语义错误。
- 自定义 `sqlTransfer` 只有在能证明 COUNT SQL替换安全时才允许下推，否则回退。
- driver 和所有 executor 必须使用同一个新 JAR。

## 测试设计

### 纯函数测试

- 单列、多列、大小写和 quoted 分布键解析；
- output schema 已含全部分布键时不补列；
- 缺失分布键时从 source schema 补齐；
- 无法解析分布键时选择随机分布兜底；
- transfer row 正确投影为 output row，包括零字段输出。

### 聚合下推测试

- 无分组 `CountStar` 且 filter 全部下推时接受；
- 存在 `GROUP BY`、多个聚合或非 CountStar 时拒绝；
- 存在未下推 filter 时拒绝；
- 生成 SQL包含原过滤条件和 `count(*)::bigint`；
- scalar reader 只规划一个分区并返回一个 `LongType` 值；
- MDB/JDBC 异常原样导致 Spark action 失败。

### 失败传播测试

- RMI registry 不存在时 read reader 构造失败；
- check-in 收到数据库失败原因时立即抛错；
- Master 正常返回 no-work 时 reader 返回 EOF；
- driver SQL 失败后再次 planning 时抛出第一条失败；
- 正常零行查询仍返回 `0`。

### 回归测试

- 现有 `TransferAbortStateTest`、`TransferFailureTest`、`SparkSchemaLookupTest` 和 `ServerPublishMappingTest` 全部通过；
- 本地编译和远端 shaded JAR 构建成功；
- JAR 同时包含 driver、executor、RMI 和新增 aggregate scan 类；
- 在可用测试环境执行 1000 万行等价数据量的 `count()`，通过 Spark physical plan 和 MDB 日志确认完整聚合下推，结果与 MDB 直接 SQL一致。

## 交付边界

代码先在本地仓库修改并提交。完成自动化测试后，再按项目既有流程同步必要源码到 `172.16.100.143:/root/spark-greenplum-connector` 构建 shaded JAR。除非另行指定，远端只用于编译和测试，不替换客户 Spark 集群中的现有 JAR。
