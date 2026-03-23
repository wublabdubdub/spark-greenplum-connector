# Iceberg 到 YMatrix 压测方案

本文档基于你当前的实际部署形态整理：

- Spark：单节点
- YMatrix：24 个 primary segment
- 目标：先在 Iceberg 中构造大规模测试数据，再将存量迁移到 YMatrix，并持续同步增量数据

当前文档已经覆盖两阶段：

- 第一阶段：向 Iceberg 批量灌入宽表测试数据，并回灌存量到 YMatrix
- 第二阶段：继续执行随机增量写入、增量同步和双端对账

## 目录

- [1. 压测目标](#section-1)
- [2. 当前环境下的实施建议](#section-2)
- [3. 宽表模型说明](#section-3)
- [4. 已提供的脚本](#section-4)
- [5. 你需要执行的命令](#section-5)
- [5.1 进入项目目录](#section-5-1)
- [5.2 查看脚本是否存在](#section-5-2)
- [5.3 启动 spark-shell](#section-5-3)
- [5.4 在 spark-shell 中加载脚本](#section-5-4)
- [5.5 加载 Iceberg 到 YMatrix 的存量迁移脚本](#section-5-5)
- [5.6 查看 Iceberg 中是否已经生成表](#section-5-6)
- [5.7 查看某张表的写入结果](#section-5-7)
- [5.8 查看某张表是否已经成功迁移到 YMatrix](#section-5-8)
- [6. 默认参数和正式压测参数](#section-6)
- [6.1 编辑脚本参数](#section-6-1)
- [6.2 Iceberg 存量迁移脚本的正式参数](#section-6-2)
- [7. 推荐执行顺序](#section-7)
- [7.1 第一步：先跑 50 张表](#section-7-1)
- [7.2 第二步：再跑正式规模](#section-7-2)
- [8. 断点续跑说明](#section-8)
- [9. 当前阶段验收标准](#section-9)
- [10. 第二阶段：增量链路压测](#section-10)
- [10.1 已补充的脚本](#section-10-1)
- [10.2 建议执行顺序](#section-10-2)
- [10.3 第二阶段默认参数](#section-10-3)
- [10.4 执行随机增量写入](#section-10-4)
- [10.5 执行增量同步到 YMatrix](#section-10-5)
- [10.6 执行对账和校验](#section-10-6)
- [11. 第二阶段验收标准](#section-11)

<a id="section-1"></a>
## 1. 压测目标

完整目标如下：

- 创建 500 张 Iceberg 宽表
- 每张表写入 1000 万行存量数据
- 存量完成后，继续随机写入增量数据
- 将存量数据回灌到 YMatrix
- 使用 watermark 机制持续同步增量数据到 YMatrix

由于当前 Spark 是单节点，建议按阶段推进，不要一开始就直接冲击 500 张表、每张 1000 万行。

<a id="section-2"></a>
## 2. 当前环境下的实施建议

单节点 Spark 的主要瓶颈通常不是 YMatrix，而是：

- Spark 单机内存
- Spark 本地磁盘 spill
- Spark 到 YMatrix 各 segment 所在节点的网络吞吐
- Iceberg 元数据和小文件数量

因此建议分 2 个阶段执行：

1. 小规模压测
   - 50 张表
   - 每张 500 万行
2. 正式压测
   - 500 张表
   - 每张 1000 万行

<a id="section-3"></a>
## 3. 宽表模型说明

本次造数采用“时序 + 分析”宽表模型，核心字段如下：

- `ingest_id`：递增主键，后续增量同步按它维护水位
- `event_time`：事件时间，用于时序分区
- `metric_01 ~ metric_20`：分析指标列
- `counter_01 ~ counter_05`：计数列
- `attr_01 ~ attr_05`：维度属性列
- `tag_01 ~ tag_03`：标签列
- `ext_json`：扩展信息列

Iceberg 表按 `days(event_time)` 分区。

<a id="section-4"></a>
## 4. 已提供的脚本

当前已经提供第一阶段两份脚本：

- [examples/iceberg-wide-table-bulk-load.scala](<repo_root>/examples/iceberg-wide-table-bulk-load.scala)
- [examples/iceberg-to-ymatrix-bulk-load.scala](<repo_root>/examples/iceberg-to-ymatrix-bulk-load.scala)

脚本能力如下：

- 自动创建 namespace
- 自动创建宽表
- 按批次生成并写入测试数据
- 支持断点续跑
  - 依据表中当前的 `max(ingest_id)` 自动继续写入
- 支持按参数逐步放大规模

第二份脚本用于把 Iceberg 宽表中的存量数据按批次迁移到 YMatrix，能力如下：

- 按表逐张回灌 `iot_wide_*`
- 自动检查目标 YMatrix 表是否存在
- 依据 YMatrix 中当前 `max(ingest_id)` 断点续跑
- 首次写入时通过 connector 自动建表
- 支持通过 `distributedby` 指定 YMatrix 分布键

<a id="section-5"></a>
## 5. 你需要执行的命令

下面命令默认在当前项目目录执行：

- 项目目录：`<repo_root>`

<a id="section-5-1"></a>
### 5.1 进入项目目录

```bash
cd <repo_root>
```

<a id="section-5-2"></a>
### 5.2 查看脚本是否存在

```bash
ls -l <repo_root>/examples/iceberg-wide-table-bulk-load.scala
ls -l <repo_root>/examples/iceberg-to-ymatrix-bulk-load.scala
```

<a id="section-5-3"></a>
### 5.3 启动 spark-shell

前提：你的 Spark 已经配置好了 Iceberg catalog。如果你已经有现成的启动命令，直接用你自己的；如果没有，可以先按下面模板启动，再把其中的 Iceberg catalog 参数替换成你自己的实际值。

```bash
cd <repo_root>

spark-shell \
  --master local[8] \
  --driver-memory 16g \
  --conf spark.sql.shuffle.partitions=8 \
  --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.local.type=hadoop \
  --conf spark.sql.catalog.local.warehouse=/data/iceberg/warehouse/ \
  --jars ./spark-ymatrix-connector_2.12-3.1.jar
```

如果你的 catalog 名称不是 `local`，需要同步修改：

- 文档中的 namespace
- 脚本中的 `icebergNamespace`

例如当前脚本默认：

```scala
val icebergNamespace = "local.test_db"
```

<a id="section-5-4"></a>
### 5.4 在 spark-shell 中加载脚本

进入 `spark-shell` 后执行：

```scala
:load examples/iceberg-wide-table-bulk-load.scala
```

<a id="section-5-5"></a>
### 5.5 加载 Iceberg 到 YMatrix 的存量迁移脚本

前提：

- 已经完成前面的 Iceberg 造数
- `spark-shell` 启动时已经带上 connector jar
- YMatrix 账号已经具备 connector 所需权限

进入 `spark-shell` 后执行：

```scala
:load examples/iceberg-to-ymatrix-bulk-load.scala
```

脚本默认会搬运：

- `local.test_db.iot_wide_0001` 到 `public.iot_wide_0001`
- `local.test_db.iot_wide_0002` 到 `public.iot_wide_0002`
- ...

默认参数如下：

```scala
val tableCount = 10
val rowsPerBatch = 200000L
val writePartitions = 8
```

如果你前面已经把 Iceberg 造数切到 50 张表或 500 张表，这里也需要把同样的 `tableCount` 调整过来。

<a id="section-5-6"></a>
### 5.6 查看 Iceberg 中是否已经生成表

在 `spark-shell` 中执行：

```scala
spark.sql("show tables in local.test_db").show(200, false)
```

<a id="section-5-7"></a>
### 5.7 查看某张表的写入结果

在 `spark-shell` 中执行：

```scala
spark.sql("select count(*) from local.test_db.iot_wide_0001").show(false)
spark.sql("select min(ingest_id), max(ingest_id) from local.test_db.iot_wide_0001").show(false)
spark.sql("select * from local.test_db.iot_wide_0001 limit 5").show(false)
```

<a id="section-5-8"></a>
### 5.8 查看某张表是否已经成功迁移到 YMatrix

可以在 `psql` 中执行：

```sql
\d public.iot_wide_0001
select count(*) from public.iot_wide_0001;
select min(ingest_id), max(ingest_id) from public.iot_wide_0001;
```

如果你希望在 `spark-shell` 中做简单校验，也可以执行：

```scala
spark.read
  .format("its-ymatrix")
  .option("url", "jdbc:postgresql://172.16.100.29:5432/zhangchen")
  .option("user", "zhangchen")
  .option("password", "YMatrix@123")
  .option("dbtable", "select count(*)::bigint as cnt from public.iot_wide_0001")
  .load()
  .show(false)
```

如果你需要把读取结果赋值给变量，注意在 `spark-shell` 里要把整条链式调用一次性赋给 `val`，不要先执行 `val ymatrixDf = spark.read` 再单独输入后续 `.option(...)`。否则 `ymatrixDf` 会是 `DataFrameReader`，后面无法调用 `printSchema()`、`count()`、`show()` 或 `writeTo()`。

正确示例：

```scala
val ymatrixDf = (
  spark.read
    .format("its-ymatrix")
    .option("url", "jdbc:postgresql://172.16.100.29:5432/zhangchen")
    .option("user", "zhangchen")
    .option("password", "YMatrix@123")
    .option("dbschema", "public")
    .option("dbtable", "iceberg_sync_demo")
    .load()
)

ymatrixDf.printSchema()
ymatrixDf.count()
ymatrixDf.show(5, false)
ymatrixDf.writeTo("local.test_db.iceberg_sync_demo").append()
```

<a id="section-6"></a>
## 6. 默认参数和正式压测参数

当前脚本默认参数如下：

```scala
val tableCount = 10
val totalRowsPerTable = 10000000L
val rowsPerBatch = 500000L
val sparkPartitions = 1
```

如果你要切到正式目标，请先编辑脚本顶部参数，再重新执行 `spark-shell` 和 `:load`。

<a id="section-6-1"></a>
### 6.1 编辑脚本参数

```bash
vi <repo_root>/examples/iceberg-wide-table-bulk-load.scala
```

把以下参数改成正式压测值：

```scala
val tableCount = 500
val totalRowsPerTable = 10000000L
val rowsPerBatch = 500000L
val sparkPartitions = 8
```

如果单机资源充足，也可以尝试：

```scala
val rowsPerBatch = 1000000L
val sparkPartitions = 12
```

但建议先从保守值开始。

<a id="section-6-2"></a>
### 6.2 Iceberg 存量迁移脚本的正式参数

如果你要把 Iceberg 宽表回灌到 YMatrix，可以编辑：

```bash
vi <repo_root>/examples/iceberg-to-ymatrix-bulk-load.scala
```

先从下面参数开始：

```scala
val tableCount = 10
val rowsPerBatch = 200000L
val writePartitions = 8
```

如果运行稳定，再切到：

```scala
val tableCount = 50
val rowsPerBatch = 200000L
val writePartitions = 8
```

最后再根据单机资源情况尝试：

```scala
val tableCount = 500
val rowsPerBatch = 200000L
val writePartitions = 8
```

如果网络和 YMatrix 写入都比较稳定，可以逐步尝试把 `rowsPerBatch` 提高到 `500000L`。

<a id="section-7"></a>
## 7. 推荐执行顺序

建议按下面顺序推进：

<a id="section-7-1"></a>
### 第一步：先跑 50 张表

修改脚本参数：

```scala
val tableCount = 50
val totalRowsPerTable = 5000000L
val rowsPerBatch = 500000L
val sparkPartitions = 8
```

重新启动并执行：

```bash
cd <repo_root>

spark-shell \
  --master local[8] \
  --driver-memory 16g \
  --conf spark.sql.shuffle.partitions=8 \
  --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.local.type=hadoop \
  --conf spark.sql.catalog.local.warehouse=/data/iceberg/warehouse/ \
  --jars ./spark-ymatrix-connector_2.12-3.1.jar
```

在 `spark-shell` 中执行：

```scala
:load examples/iceberg-wide-table-bulk-load.scala
:load examples/iceberg-to-ymatrix-bulk-load.scala
```

<a id="section-7-2"></a>
### 第二步：再跑正式规模

修改脚本参数：

```scala
val tableCount = 500
val totalRowsPerTable = 10000000L
val rowsPerBatch = 500000L
val sparkPartitions = 8
```

重新启动并执行：

```bash
cd <repo_root>

spark-shell \
  --master local[8] \
  --driver-memory 16g \
  --conf spark.sql.shuffle.partitions=8 \
  --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.local.type=hadoop \
  --conf spark.sql.catalog.local.warehouse=/data/iceberg/warehouse/ \
  --jars ./spark-ymatrix-connector_2.12-3.1.jar
```

在 `spark-shell` 中执行：

```scala
:load examples/iceberg-wide-table-bulk-load.scala
:load examples/iceberg-to-ymatrix-bulk-load.scala
```

<a id="section-8"></a>
## 8. 断点续跑说明

脚本支持续跑，不会每次都从头开始写。

核心逻辑是：

- 每张表先读取当前 `max(ingest_id)`
- 新一轮从 `max(ingest_id) + 1` 开始写
- 达到目标 `totalRowsPerTable` 就停止

因此如果中途失败，重新执行同一个脚本即可继续。

Iceberg 到 YMatrix 的存量迁移脚本也是同样思路：

- 先读取 Iceberg 源表当前 `max(ingest_id)`
- 再读取 YMatrix 目标表当前 `max(ingest_id)`
- 从 `gp_max + 1` 开始补写缺失区间
- 如果 YMatrix 已经追平 Iceberg，则自动跳过

<a id="section-9"></a>
## 9. 当前阶段验收标准

本阶段的验收标准如下：

- 可以自动创建 Iceberg namespace
- 可以自动创建宽表
- 可以按批次向表中写入数据
- 支持中断后续跑
- 能够完成目标规模下的 Iceberg 数据写入
- 能够把至少 1 张 Iceberg 宽表完整迁移到 YMatrix
- 迁移完成后，Iceberg 与 YMatrix 的 `count(*)` 和 `max(ingest_id)` 一致

<a id="section-10"></a>
## 10. 第二阶段：增量链路压测

第一阶段已经覆盖：

- Iceberg 宽表批量造数
- Iceberg 存量批量迁移到 YMatrix

现在继续落第二阶段，目标是把“增量写入 -> 增量同步 -> 对账校验”完整串起来。

这一阶段新增的核心要求如下：

- 在已有 `iot_wide_*` Iceberg 表上持续追加随机增量数据
- 按 `ingest_id` 水位把新增部分增量同步到 YMatrix
- 在 YMatrix 中维护 watermark，便于持续同步和断点续跑
- 在同步后执行双端对账，校验 `count(*)`、`min(ingest_id)`、`max(ingest_id)` 和抽样行内容

<a id="section-10-1"></a>
### 10.1 已补充的脚本

第二阶段已经补充以下 3 份脚本：

- [examples/iceberg-wide-table-random-increment-load.scala](<repo_root>/examples/iceberg-wide-table-random-increment-load.scala)
- [examples/iceberg-to-ymatrix-incremental-sync.scala](<repo_root>/examples/iceberg-to-ymatrix-incremental-sync.scala)
- [examples/iceberg-ymatrix-reconciliation.scala](<repo_root>/examples/iceberg-ymatrix-reconciliation.scala)

它们的职责分别是：

1. `iceberg-wide-table-random-increment-load.scala`
   - 对 `iot_wide_*` 表按轮次随机追加增量数据
   - 每张表从当前 `max(ingest_id) + 1` 自动续写
   - 可通过 `totalRounds` 和 `pauseSeconds` 控制一次性执行或持续执行
2. `iceberg-to-ymatrix-incremental-sync.scala`
   - 对 `iot_wide_*` 表按 `ingest_id` 增量同步到 YMatrix
   - 每张表分批同步，支持断点续跑
   - 自动维护 watermark 表 `public.iceberg_iot_wide_sync_watermark`
   - 同时兼容“watermark 落后但目标表已写入”的场景，会自动以 `max(watermark, target max)` 作为续跑点
3. `iceberg-ymatrix-reconciliation.scala`
   - 对比 Iceberg 与 YMatrix 两端的 `count(*)`
   - 对比 `min(ingest_id)` 与 `max(ingest_id)`
   - 可选做抽样整行对账，默认抽样 3 个 `ingest_id`

<a id="section-10-2"></a>
### 10.2 建议执行顺序

建议按下面顺序推进第二阶段：

1. 先完成第一阶段的存量造数和存量迁移
2. 执行随机增量写入脚本，向 Iceberg 宽表追加若干轮增量
3. 执行增量同步脚本，把新增 `ingest_id` 区间同步到 YMatrix
4. 执行对账脚本，确认双端结果一致
5. 如果要做持续压测，可以把“随机增量写入”和“增量同步”都改成多轮或持续运行

建议先从 10 张表开始，确认链路稳定后，再放大到 50 张表、500 张表。

<a id="section-10-3"></a>
### 10.3 第二阶段默认参数

随机增量写入脚本默认参数如下：

```scala
val tableCount = 10
val minRowsPerTablePerRound = 10000L
val maxRowsPerTablePerRound = 50000L
val totalRounds = 5
val pauseSeconds = 0
```

增量同步脚本默认参数如下：

```scala
val tableCount = 10
val rowsPerBatch = 100000L
val writePartitions = 8
val maxSyncPasses = 1
val pauseSeconds = 0
val ymatrixWatermarkTable = "iceberg_iot_wide_sync_watermark"
```

对账脚本默认参数如下：

```scala
val tableCount = 10
val sampleIdCount = 3
val includeRowSampleCheck = true
```

如果你前面已经把第一阶段放大到 50 张表或 500 张表，这里也需要把第二阶段脚本里的 `tableCount` 调整成相同数量。

<a id="section-10-4"></a>
### 10.4 执行随机增量写入

继续使用前面已经启动好的 `spark-shell`，执行：

```scala
:load examples/iceberg-wide-table-random-increment-load.scala
```

执行成功后，你会在日志中看到类似输出：

```text
[round] start round=1, tableCount=10
[batch] round=1, table=local.test_db.iot_wide_0001, rows=..., ingest_id=[..., ...]
[round] finished round=1
```

你也可以在 `spark-shell` 中抽查某张表：

```scala
spark.sql("select count(*) from local.test_db.iot_wide_0001").show(false)
spark.sql("select min(ingest_id), max(ingest_id) from local.test_db.iot_wide_0001").show(false)
```

如果希望持续写入，可以把脚本里的：

```scala
val totalRounds = 5
```

改成：

```scala
val totalRounds = -1
val pauseSeconds = 10
```

这样脚本会每隔 10 秒继续跑下一轮增量写入。

<a id="section-10-5"></a>
### 10.5 执行增量同步到 YMatrix

在同一个 `spark-shell` 中继续执行：

```scala
:load examples/iceberg-to-ymatrix-incremental-sync.scala
```

这个脚本会做以下几件事：

- 自动确保 watermark 表存在
- 对每张 `iot_wide_*` 表读取 Iceberg 当前 `max(ingest_id)`
- 对每张目标表读取当前 watermark 和目标端 `max(ingest_id)`
- 从 `max(watermark, target max) + 1` 开始补同步
- 每个批次成功后更新 watermark

第一次执行后，可以到 YMatrix 中查看 watermark：

```sql
select *
from public.iceberg_iot_wide_sync_watermark
order by table_name;
```

如果你希望把增量同步改成持续运行，可以把脚本里的：

```scala
val maxSyncPasses = 1
```

改成：

```scala
val maxSyncPasses = -1
val pauseSeconds = 10
```

这样脚本会每隔 10 秒再次扫描一轮所有宽表，继续推进水位。

<a id="section-10-6"></a>
### 10.6 执行对账和校验

在 `spark-shell` 中执行：

```scala
:load examples/iceberg-ymatrix-reconciliation.scala
```

默认会输出每张表的对账结果，例如：

```text
[pass] local.test_db.iot_wide_0001 == public.iot_wide_0001, rowCount=..., ingest_id=[..., ...]
```

如果失败，会输出源端和目标端统计差异，例如：

```text
[fail] local.test_db.iot_wide_0001 != public.iot_wide_0001, source={...}, target={...}
```

如果你只想做轻量统计校验，可以把脚本里的：

```scala
val includeRowSampleCheck = true
```

改成：

```scala
val includeRowSampleCheck = false
```

这样只对比双端统计值，不做整行抽样比对。

<a id="section-11"></a>
## 11. 第二阶段验收标准

第二阶段建议按以下标准验收：

- 可以在已有 `iot_wide_*` Iceberg 表上持续追加随机增量数据
- 增量写入脚本支持从当前 `max(ingest_id)` 自动续写
- 可以把新增数据按 `ingest_id` 增量同步到 YMatrix
- 增量同步脚本支持分批执行和断点续跑
- YMatrix watermark 表能够正确记录每张表的同步进度
- 再次执行同步脚本时，不会重复回灌已经同步完成的历史区间
- 对账脚本能够输出通过、失败和跳过的结果
- 至少抽查 1 张表时，Iceberg 与 YMatrix 的 `count(*)`、`min(ingest_id)`、`max(ingest_id)` 一致
- 在启用抽样比对时，抽样行内容一致
