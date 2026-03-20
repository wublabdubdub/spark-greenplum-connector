# Iceberg 到 YMatrix 压测方案

本文档基于你当前的实际部署形态整理：

- Spark：单节点
- YMatrix：24 个 primary segment
- 目标：先在 Iceberg 中构造大规模测试数据，再将存量迁移到 YMatrix，并持续同步增量数据

当前先落第一阶段：向 Iceberg 批量灌入宽表测试数据。

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
- [10. 下一步计划](#section-10)

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

当前已经提供第一份脚本：

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
val totalRowsPerTable = 1000000L
val rowsPerBatch = 500000L
val sparkPartitions = 8
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
## 10. 下一步计划

在这一步跑通后，下一阶段建议继续补充以下脚本：

1. Iceberg 随机增量写入脚本
2. Iceberg 增量同步到 YMatrix 的脚本
3. 对账和校验脚本
