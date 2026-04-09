# Iceberg 到 YMatrix 标准开发手册
## 基于 `spark-ymatrix-connector` 的开发接入、批量迁移与增量同步指南

| 项 | 说明 |
|---|---|
| 覆盖范围 | 使用本仓库提供的 connector，将 Iceberg 数据迁移到 YMatrix |
| 文档类型 | 开发手册 |
| 适用对象 | Spark 开发人员、数据平台开发人员、数据集成开发人员 |
| 版本基线 | Spark 3.4.1 / Scala 2.12.17 / Java 1.8 / Connector 3.1.0 |
| 代码基线 | 当前仓库 `spark-ymatrix-connector` |
| JAR 提供方式 | 由平台侧或交付侧直接提供已编译好的 JAR |
| 文档日期 | 2026-03-19 |

## 目录

1. [概述](#1-概述)
2. [核心定位与典型场景](#2-核心定位与典型场景)
3. [架构与核心概念](#3-架构与核心概念)
4. [开发前准备](#4-开发前准备)
5. [快速开始](#5-快速开始)
6. [开发实战与完整示例](#6-开发实战与完整示例)
7. [深入原理与性能调优](#7-深入原理与性能调优)
8. [高级功能与扩展方式](#8-高级功能与扩展方式)
9. [安全、权限与常见误区](#9-安全权限与常见误区)
10. [附录](#10-附录)

## 1. 概述

### 1.1 组件说明

本项目是一个 Spark DataSource V2 Connector，可直接嵌入 Spark 作业，用于读写 YMatrix。

在 Iceberg 到 YMatrix 的场景中，标准开发链路如下：

1. Spark 读取 Iceberg 表
2. Spark 对 DataFrame 做字段选择、过滤、转换、重分区
3. Spark 通过 connector 将 DataFrame 写入 YMatrix

开发接入时的核心入口只有一个：

```scala
.format("its-ymatrix")
```

### 1.2 适用问题

Iceberg 到 YMatrix 的迁移通常涉及以下开发问题：

- 需要在 Spark 内部直接完成离线迁移，不希望额外引入独立同步系统
- 需要按业务字段进行过滤、重命名、映射和类型转换
- 需要可控地按批写入，以支持大表迁移和断点续跑
- 需要将迁移逻辑写成标准 Spark 作业，而不是临时脚本堆砌
- 需要在 Spark 内直接完成结果校验与联调

本 connector 用于处理上述 Spark 作业内嵌式迁移场景。

### 1.3 边界

本 connector 不提供以下能力：

- 自动 CDC 平台
- 自动元数据同步平台
- 自动表结构演进治理平台
- 自动去重补偿平台
- 自动增量语义保障平台

字段映射、幂等控制、水位推进、重跑策略和目标表治理仍需由开发代码或外围流程实现。

## 2. 核心定位与典型场景

### 2.1 组件定位

从架构上看，这个 connector 处于 Spark 到 YMatrix 写入链路中的连接层：

```text
+-------------------------------------------------------------+
| 业务迁移逻辑层                                               |
| 过滤、字段映射、分批、断点续跑、校验、调度                  |
+-------------------------------------------------------------+
| Spark 计算与数据编排层                                       |
| Iceberg 读取 / DataFrame 转换 / repartition / SQL            |
+-------------------------------------------------------------+
| YMatrix 写入连接层                                           |
| format("its-ymatrix") + GPFDIST + JDBC 元数据交互          |
+-------------------------------------------------------------+
```

### 2.2 典型场景

典型场景包括：

| 场景 | 支持情况 | 说明 |
|---|---|---|
| 单表一次性迁移 | 支持 | 可用于开发验证和小规模上线 |
| 多表批量回灌 | 支持 | 可配合脚本按表循环执行 |
| 大表按水位字段分批迁移 | 支持 | 通常使用 `ingest_id`、`order_id` 等单调字段 |
| 增量同步演示 | 支持 | 可基于业务水位做简化同步 |
| 强一致 CDC | 不直接提供 | 需要外围机制配合 |
| 自动 DDL 治理 | 不直接提供 | 需要开发侧自行约束 |

### 2.3 使用顺序

典型使用顺序如下：

1. 先用最小 Demo 验证环境和权限
2. 再用单表迁移模板验证字段映射和类型兼容
3. 再执行分批迁移或多表迁移
4. 最后再实现增量同步或持续运行任务

## 3. 架构与核心概念

### 3.1 整体架构

开发视角下，Iceberg 到 YMatrix 的标准链路如下：

```text
                    +----------------------+
                    |   Iceberg Catalog    |
                    |  (Hadoop/Hive/REST)  |
                    +----------+-----------+
                               |
                               v
                    +----------------------+
                    |      Spark SQL       |
                    |  table / SQL / DF    |
                    +----------+-----------+
                               |
                     select / filter / cast
                     repartition / batching
                               |
                               v
                    +----------------------+
                    | its-ymatrix sink     |
                    | Connector DataSource |
                    +----------+-----------+
                               |
                     JDBC metadata + GPFDIST
                               |
                               v
                    +----------------------+
                    |       YMatrix        |
                    | schema / table / seg |
                    +----------------------+
```

### 3.2 核心概念

#### 3.2.1 `dbschema`

`dbschema` 用于指定目标 schema。当前实现中，如果 schema 不存在，connector 会尝试自动创建。

这意味着开发阶段可以降低初次接入门槛；生产环境通常需要预先规划 schema。

#### 3.2.2 `dbtable`

`dbtable` 在写入时表示目标表名；在读取时既可以是目标表名，也可以是一段 SQL 查询。

这使得开发人员可以直接在 Spark 中用 connector 反查 YMatrix 的结果，而不必每次切换到 `psql`。

#### 3.2.3 `distributedby`

`distributedby` 影响目标表在 YMatrix 中的分布方式。它不是普通参数，而是直接决定后续数据分布、倾斜情况和部分查询性能的关键参数。

分布键选择条件：

- 选择高基数字段
- 选择写入与查询都较稳定的字段
- 避免明显热点字段

#### 3.2.4 `mode("append")` 与 `mode("overwrite")`

二者区别如下：

| 模式 | 适用场景 | 风险点 |
|---|---|---|
| `append` | 增量写入、分批写入、断点续跑 | 无幂等设计时可能重复写入 |
| `overwrite` | 全量重灌、覆盖测试表 | 可能重建表或清空旧数据 |
| `overwrite + truncate=true` | 保留表结构重新导入 | 仍需确认表结构与当前数据兼容 |

#### 3.2.5 断点续跑

断点续跑不是 connector 自动提供的“业务语义”，而是开发代码基于业务水位字段实现的控制逻辑。

当前仓库中的标准方式是：

1. 查询 Iceberg 源表最大水位
2. 查询 YMatrix 目标表最大水位
3. 只继续写入尚未同步的水位区间

适合做水位的字段通常包括：

- `ingest_id`
- `order_id`
- 自增流水号
- 单调递增的业务批次号

如果你的业务表不存在稳定的单调字段，则需要另行设计幂等策略。

## 4. 开发前准备

### 4.1 环境前提

本文不讨论 JAR 打包过程，默认前提如下：

| 项 | 要求 |
|---|---|
| Spark | 3.x |
| Java | 1.8 |
| Iceberg | 已在 Spark 中配置 catalog |
| Connector JAR | 已由平台统一提供 |
| YMatrix | 已可通过 JDBC 正常连接 |
| 网络 | YMatrix segment 可访问 Spark Worker |

### 4.3 统一环境变量

本文中的示例统一使用以下环境变量：

```bash
export CONNECTOR_JAR=/path/to/spark-ymatrix-connector_2.12-3.1.jar
export YMATRIX_URL=jdbc:postgresql://ymatrix-master-host:5432/your_database
export YMATRIX_USER=database_user
export YMATRIX_PASSWORD=yourpassword
export YMATRIX_SCHEMA=public
export ICEBERG_CATALOG=local
export ICEBERG_WAREHOUSE=/tmp/iceberg-warehouse
export SPARK_LOCAL_IP=127.0.0.1
```

### 4.4 `spark-shell` 启动模板

```bash
spark-shell \
  --master local[4] \
  --driver-memory 8g \
  --conf spark.driver.host=${SPARK_LOCAL_IP} \
  --conf spark.driver.bindAddress=0.0.0.0 \
  --conf spark.local.ip=${SPARK_LOCAL_IP} \
  --conf spark.sql.shuffle.partitions=4 \
  --conf spark.sql.catalog.${ICEBERG_CATALOG}=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.${ICEBERG_CATALOG}.type=hadoop \
  --conf spark.sql.catalog.${ICEBERG_CATALOG}.warehouse=${ICEBERG_WAREHOUSE} \
  --jars ${CONNECTOR_JAR}
```

### 4.5 启动后的自检

进入 `spark-shell` 后可先执行以下命令：

```scala
val catalog = sys.env.getOrElse("ICEBERG_CATALOG", "local")
spark.sql("show databases").show(false)
println(sys.env.getOrElse("YMATRIX_URL", "YMATRIX_URL_NOT_SET"))
println(s"catalog = $catalog")
```

如果这里失败，问题仍位于环境层。

## 5. 快速开始

### 5.1 最小接入代码模型

最小接入代码模型如下：

```scala
spark.table("catalog.db.source_table")
  .write
  .format("its-ymatrix")
  .option("url", "jdbc:postgresql://host:5432/db")
  .option("user", "database_user")
  .option("password", "yourpassword")
  .option("dbschema", "public")
  .option("dbtable", "target_table")
  .mode("append")
  .save()
```

该模型包含以下技术要点：

- 源侧是 Spark DataFrame
- 目标侧是 `its-ymatrix` sink
- 字段选择、过滤、转换和重分区发生在 Spark 中

### 5.2 SQL 接入模型：`CREATE TEMPORARY VIEW` + `INSERT INTO`

在启用 Iceberg `SparkCatalog` 的环境中，`CREATE TABLE ... USING its-ymatrix` 会进入 catalog create-table 流程，不适合作为当前 connector 的 SQL 接入方式。可用的 SQL 方式是先创建临时 sink view，再执行 `INSERT INTO TABLE ... SELECT ...`。

```scala
spark.sql("DROP VIEW IF EXISTS ymatrix_sink")

spark.sql(
  """
    |CREATE TEMPORARY VIEW ymatrix_sink
    |USING com.itsumma.gpconnector.GreenplumDataSource
    |OPTIONS (
    |  url 'jdbc:postgresql://ymatrix-master-host:5432/your_database',
    |  user 'database_user',
    |  password 'yourpassword',
    |  dbschema 'public',
    |  dbtable 'target_table',
    |  distributedby 'ingest_id',
    |  network.timeout '120s',
    |  server.timeout '120s',
    |  dbmessages 'WARN'
    |)
    |""".stripMargin
)

spark.sql(
  s"""
     |INSERT INTO TABLE ymatrix_sink
     |SELECT *
     |FROM ${sys.env.getOrElse("ICEBERG_CATALOG", "local")}.test_db.iot_wide_0001
     |""".stripMargin
)
```

这个写法要求目标表已经存在，因为 `CREATE TEMPORARY VIEW` 会通过 provider 访问目标端并推断 schema。

### 5.3 开发接入的最小参数集

| 参数 | 参数角色 | 说明 |
|---|---|---|
| `url` | 必填 | YMatrix JDBC 地址 |
| `user` | 必填 | 用户名 |
| `password` | 必填 | 密码 |
| `dbschema` | 必填 | 目标 schema |
| `dbtable` | 必填 | 目标表 |
| `mode` | 必填 | 写入模式，取值为 `append` 或 `overwrite` |
| `distributedby` | 自动建表时填写 | 自动建表时指定分布键 |
| `network.timeout` | 可选 | 网络敏感环境可显式设定 |
| `server.timeout` | 可选 | 大批量传输时可显式设定 |

### 5.4 开发控制项

开发代码至少需要明确以下三项：

1. 源表字段清单明确
2. 目标表写入模式明确
3. 水位与幂等策略明确

## 6. 开发实战与完整示例

本节是本文重点。所有示例都按“场景说明、完整代码、结果校验、注意事项”展开。
为便于新人直接照着理解和改造，下面所有 Demo 代码均采用逐行注释方式编写。

### 6.1 示例一：最小可复现链路

#### 6.1.1 场景说明

这个示例用于验证最小闭环：

- Spark 能写 Iceberg
- Spark 能通过 connector 写 YMatrix
- Spark 能再从 YMatrix 读回结果

#### 6.1.2 完整代码

```scala
// 导入 spark implicits，便于使用 Seq.toDF 等隐式转换能力。
import spark.implicits._
// 导入 DataFrame 常用函数，例如 col。
import org.apache.spark.sql.functions._

// 读取 Iceberg catalog 名称；若未设置环境变量，则默认使用 local。
val catalog = sys.env.getOrElse("ICEBERG_CATALOG", "local")
// 读取 YMatrix JDBC 地址。
val ymatrixUrl = sys.env("YMATRIX_URL")
// 读取 YMatrix 用户名。
val ymatrixUser = sys.env("YMATRIX_USER")
// 读取 YMatrix 密码。
val ymatrixPassword = sys.env("YMATRIX_PASSWORD")
// 读取目标 schema；若未设置，则默认写入 public。
val ymatrixSchema = sys.env.getOrElse("YMATRIX_SCHEMA", "public")

// 组织 Iceberg namespace 名称。
val icebergNamespace = s"${catalog}.demo_db"
// 组织 Iceberg 测试表全名。
val icebergTable = s"${icebergNamespace}.orders_demo"
// 指定 YMatrix 目标表名。
val ymatrixTable = "orders_demo"

// 如果测试 namespace 不存在，则先创建。
spark.sql(s"create namespace if not exists ${icebergNamespace}")

// 构造 3 行最小测试数据，并指定字段顺序。
val srcDf = Seq(
  // 第 1 行测试订单数据。
  (1L, "u001", BigDecimal("18.50"), "2026-03-19 10:00:00"),
  // 第 2 行测试订单数据。
  (2L, "u002", BigDecimal("20.00"), "2026-03-19 10:05:00"),
  // 第 3 行测试订单数据。
  (3L, "u003", BigDecimal("99.99"), "2026-03-19 10:10:00")
// 将本地集合转换为 Spark DataFrame，并显式指定列名。
).toDF("order_id", "user_id", "amount", "created_at")
  // 将字符串时间列转换成 timestamp 类型，便于写入目标库。
  .withColumn("created_at", col("created_at").cast("timestamp"))

// 将测试数据写入 Iceberg 表；若表已存在则直接替换，便于重复联调。
srcDf.writeTo(icebergTable).createOrReplace()

// 读取刚刚创建的 Iceberg 表。
spark.table(icebergTable)
  // 切换到 DataFrameWriter。
  .write
  // 指定使用当前 connector 作为写入数据源。
  .format("its-ymatrix")
  // 指定 YMatrix JDBC 地址。
  .option("url", ymatrixUrl)
  // 指定 YMatrix 用户名。
  .option("user", ymatrixUser)
  // 指定 YMatrix 密码。
  .option("password", ymatrixPassword)
  // 指定目标 schema。
  .option("dbschema", ymatrixSchema)
  // 指定目标表名。
  .option("dbtable", ymatrixTable)
  // 指定自动建表时的分布键。
  .option("distributedby", "order_id")
  // 固定 GPFDIST 服务端口，便于排查网络问题。
  .option("server.port", "43000")
  // 设置内部网络超时时间。
  .option("network.timeout", "60s")
  // 设置 GPFDIST 传输超时时间。
  .option("server.timeout", "60s")
  // 覆盖写入目标表，便于重复执行示例。
  .mode("overwrite")
  // 真正执行写入动作。
  .save()

// 通过 connector 再次从 YMatrix 读取目标表，验证写入结果。
val verifyDf = (
  // 创建读取入口。
  spark.read
    // 指定使用当前 connector 读取 YMatrix。
    .format("its-ymatrix")
    // 指定 JDBC 地址。
    .option("url", ymatrixUrl)
    // 指定用户名。
    .option("user", ymatrixUser)
    // 指定密码。
    .option("password", ymatrixPassword)
    // 指定查询 SQL，并按 order_id 排序便于观察结果。
    .option("dbtable", s"select * from ${ymatrixSchema}.${ymatrixTable} order by order_id")
    // 执行读取。
    .load()
)

// 直接打印查询结果，不截断字段。
verifyDf.show(false)
// 输出结果总行数，确认是否为 3。
println(s"rowCount = ${verifyDf.count()}")
```

#### 6.1.3 预期结果

```text
+--------+-------+------+-------------------+
|order_id|user_id|amount|created_at         |
+--------+-------+------+-------------------+
|1       |u001   |18.50 |2026-03-19 10:00:00|
|2       |u002   |20.00 |2026-03-19 10:05:00|
|3       |u003   |99.99 |2026-03-19 10:10:00|
+--------+-------+------+-------------------+
rowCount = 3
```

#### 6.1.4 注意事项

- 首次联调通常使用 `overwrite`
- 目标表若已存在旧结构，可能影响结果
- 如需固定网络排查，可显式指定 `server.port`

### 6.2 示例二：迁移已有 Iceberg 表

#### 6.2.1 场景说明

这是最接近真实业务开发的基础模板。它不负责造数据，而是直接迁移已经存在的 Iceberg 表。

#### 6.2.2 设计原则

显式列清单与 `select("*")` 相比有以下特征：

- 源表字段顺序可能变化
- 后续新增列可能破坏目标表兼容性
- 宽表迁移时显式列清单更利于代码评审和问题定位

#### 6.2.3 完整代码

```scala
// 导入列选择函数 col，便于构造显式字段清单。
import org.apache.spark.sql.functions.col

// 读取 Iceberg catalog 名称；未指定时默认 local。
val catalog = sys.env.getOrElse("ICEBERG_CATALOG", "local")
// 组织 Iceberg 源表全名。
val sourceTable = s"${catalog}.test_db.iot_wide_0001"
// 指定 YMatrix 目标表名。
val targetTable = "iot_wide_0001"

// 读取宽表，并显式列出所有需要迁移的字段。
val wideDf = spark.table(sourceTable)
  // 通过 select 固定列顺序，避免使用 select(*) 带来的结构漂移风险。
  .select(
    // 单调递增水位字段，后续可用于分批迁移和断点续跑。
    col("ingest_id"),
    // 业务事件唯一标识。
    col("event_id"),
    // 设备标识。
    col("device_id"),
    // 租户标识。
    col("tenant_id"),
    // 站点标识。
    col("site_id"),
    // 产线标识。
    col("line_id"),
    // 区域编码。
    col("region_code"),
    // 事件时间。
    col("event_time"),
    // 事件日期。
    col("event_date"),
    // 事件分钟粒度字段。
    col("event_minute"),
    // 状态码。
    col("status_code"),
    // 告警等级。
    col("alarm_level"),
    // 宽表指标字段 01。
    col("metric_01"),
    // 宽表指标字段 02。
    col("metric_02"),
    // 宽表指标字段 03。
    col("metric_03"),
    // 宽表指标字段 04。
    col("metric_04"),
    // 宽表指标字段 05。
    col("metric_05"),
    // 宽表指标字段 06。
    col("metric_06"),
    // 宽表指标字段 07。
    col("metric_07"),
    // 宽表指标字段 08。
    col("metric_08"),
    // 宽表指标字段 09。
    col("metric_09"),
    // 宽表指标字段 10。
    col("metric_10"),
    // 宽表指标字段 11。
    col("metric_11"),
    // 宽表指标字段 12。
    col("metric_12"),
    // 宽表指标字段 13。
    col("metric_13"),
    // 宽表指标字段 14。
    col("metric_14"),
    // 宽表指标字段 15。
    col("metric_15"),
    // 宽表指标字段 16。
    col("metric_16"),
    // 宽表指标字段 17。
    col("metric_17"),
    // 宽表指标字段 18。
    col("metric_18"),
    // 宽表指标字段 19。
    col("metric_19"),
    // 宽表指标字段 20。
    col("metric_20"),
    // 计数字段 01。
    col("counter_01"),
    // 计数字段 02。
    col("counter_02"),
    // 计数字段 03。
    col("counter_03"),
    // 计数字段 04。
    col("counter_04"),
    // 计数字段 05。
    col("counter_05"),
    // 属性字段 01。
    col("attr_01"),
    // 属性字段 02。
    col("attr_02"),
    // 属性字段 03。
    col("attr_03"),
    // 属性字段 04。
    col("attr_04"),
    // 属性字段 05。
    col("attr_05"),
    // 标签字段 01。
    col("tag_01"),
    // 标签字段 02。
    col("tag_02"),
    // 标签字段 03。
    col("tag_03"),
    // 扩展 JSON 字段。
    col("ext_json")
  )
  // 在写入前重分区，控制 Spark 写入并发。
  .repartition(4)

// 将宽表数据写入 YMatrix。
wideDf.write
  // 指定使用当前 connector 写入。
  .format("its-ymatrix")
  // 指定 JDBC 地址。
  .option("url", sys.env("YMATRIX_URL"))
  // 指定用户名。
  .option("user", sys.env("YMATRIX_USER"))
  // 指定密码。
  .option("password", sys.env("YMATRIX_PASSWORD"))
  // 指定目标 schema。
  .option("dbschema", sys.env.getOrElse("YMATRIX_SCHEMA", "public"))
  // 指定目标表名。
  .option("dbtable", targetTable)
  // 指定目标分布键。
  .option("distributedby", "ingest_id")
  // 设置网络超时。
  .option("network.timeout", "120s")
  // 设置服务端超时。
  .option("server.timeout", "120s")
  // 以追加模式写入。
  .mode("append")
  // 执行写入。
  .save()
```

#### 6.2.4 实现要点

- 对宽表显式维护字段列表
- `repartition` 需要与 Spark 并发和 YMatrix segment 数量一起评估
- 首次迁移可使用较小分区数和较小样本验证链路

### 6.3 示例三：按范围迁移，先做灰度验证

#### 6.3.1 场景说明

正式迁移前，不应直接全量导入。更合理的做法是先切一个小范围窗口验证结果。

#### 6.3.2 完整代码

```scala
// 导入 Spark 常用函数，便于做范围过滤。
import org.apache.spark.sql.functions._

// 读取 Iceberg catalog 名称；未指定时默认 local。
val catalog = sys.env.getOrElse("ICEBERG_CATALOG", "local")
// 组织 Iceberg 源表全名。
val sourceTable = s"${catalog}.test_db.iot_wide_0001"

// 读取源表后先按 ingest_id 范围过滤，再按 event_date 做日期过滤。
val incrDf = spark.table(sourceTable)
  // 只保留指定主键范围内的数据，用于灰度验证。
  .where(col("ingest_id").between(1L, 100000L))
  // 只保留指定日期之后的数据，模拟时间窗口迁移。
  .where(col("event_date") >= lit("2026-03-01"))
  // 控制写入前并发。
  .repartition(4)

// 将过滤后的灰度数据写入单独的验证表。
incrDf.write
  // 指定 connector 数据源。
  .format("its-ymatrix")
  // 指定 JDBC 地址。
  .option("url", sys.env("YMATRIX_URL"))
  // 指定用户名。
  .option("user", sys.env("YMATRIX_USER"))
  // 指定密码。
  .option("password", sys.env("YMATRIX_PASSWORD"))
  // 指定目标 schema。
  .option("dbschema", sys.env.getOrElse("YMATRIX_SCHEMA", "public"))
  // 指定灰度验证目标表。
  .option("dbtable", "iot_wide_0001_range_demo")
  // 指定分布键。
  .option("distributedby", "ingest_id")
  // 固定服务端口，便于联调定位网络问题。
  .option("server.port", "43001")
  // 使用覆盖模式，确保每次灰度验证结果干净。
  .mode("overwrite")
  // 执行写入。
  .save()
```

#### 6.3.3 适用场景

典型场景包括：

- 灰度迁移
- 小流量验证
- 某个时间窗口补数
- 某个主键范围回灌

### 6.4 示例四：字段映射与重命名

#### 6.4.1 场景说明

真实业务中，源表字段与目标表字段通常不会完全同名。本例展示如何在 Spark 中完成字段映射，再写入 YMatrix。

#### 6.4.2 完整代码

```scala
// 导入 Spark 常用函数，便于做字段映射与类型转换。
import org.apache.spark.sql.functions._

// 读取 Iceberg catalog 名称；未指定时默认 local。
val catalog = sys.env.getOrElse("ICEBERG_CATALOG", "local")

// 读取源表后，对字段进行重命名与类型映射。
val mappedDf = spark.table(s"${catalog}.demo_db.orders_demo")
  // 只保留目标表需要的字段，并完成名称转换。
  .select(
    // 将 order_id 映射为业务侧主键字段 biz_order_id。
    col("order_id").as("biz_order_id"),
    // 将 user_id 映射为 buyer_id。
    col("user_id").as("buyer_id"),
    // 将金额字段显式转换为 decimal(18,2)。
    col("amount").cast("decimal(18,2)").as("pay_amount"),
    // 将时间字段改名为 pay_time。
    col("created_at").as("pay_time")
  )

// 将映射后的结果写入新表。
mappedDf.write
  // 指定 connector 数据源。
  .format("its-ymatrix")
  // 指定 JDBC 地址。
  .option("url", sys.env("YMATRIX_URL"))
  // 指定用户名。
  .option("user", sys.env("YMATRIX_USER"))
  // 指定密码。
  .option("password", sys.env("YMATRIX_PASSWORD"))
  // 指定目标 schema。
  .option("dbschema", sys.env.getOrElse("YMATRIX_SCHEMA", "public"))
  // 指定目标表名。
  .option("dbtable", "orders_demo_renamed")
  // 指定分布键。
  .option("distributedby", "biz_order_id")
  // 覆盖写入，便于多次联调。
  .mode("overwrite")
  // 执行写入。
  .save()
```

#### 6.4.3 实现约束

字段映射实现要点如下：

- 字段映射逻辑集中在一处维护
- 对关键类型显式 `cast`
- 对金额、时间、主键字段做严格映射

### 6.5 示例五：覆盖写入但保留目标表结构

#### 6.5.1 场景说明

开发环境常常需要“清空旧数据重新导入”，但并不希望把表删掉重建。

#### 6.5.2 完整代码

```scala
// 读取 Iceberg catalog 名称；未指定时默认 local。
val catalog = sys.env.getOrElse("ICEBERG_CATALOG", "local")

// 读取源表后，直接将其覆盖写入 YMatrix 目标表。
spark.table(s"${catalog}.demo_db.orders_demo")
  // 切换到 DataFrameWriter。
  .write
  // 指定 connector 数据源。
  .format("its-ymatrix")
  // 指定 JDBC 地址。
  .option("url", sys.env("YMATRIX_URL"))
  // 指定用户名。
  .option("user", sys.env("YMATRIX_USER"))
  // 指定密码。
  .option("password", sys.env("YMATRIX_PASSWORD"))
  // 指定目标 schema。
  .option("dbschema", sys.env.getOrElse("YMATRIX_SCHEMA", "public"))
  // 指定目标表名。
  .option("dbtable", "orders_demo")
  // 告诉 connector 在 overwrite 时使用 truncate，尽量保留表结构。
  .option("truncate", "true")
  // 使用覆盖模式。
  .mode("overwrite")
  // 执行写入。
  .save()
```

#### 6.5.3 与普通 `overwrite` 的区别

| 写法 | 行为特点 |
|---|---|
| `mode("overwrite")` | 可能删除并重建目标表 |
| `mode("overwrite") + truncate=true` | 尝试保留表结构并清空数据 |

### 6.6 示例六：使用 `CREATE TEMPORARY VIEW` 迁移 Iceberg 表

#### 6.6.1 场景说明

当目标表已经在 YMatrix 中预先创建完成时，可以在 `spark-shell` 中先创建临时 sink view，再通过 `INSERT INTO TABLE ... SELECT ...` 执行迁移。

#### 6.6.2 目标表准备

先在 YMatrix 中创建目标表：

```sql
create table public.orders_demo_sql_sink (
  order_id bigint,
  user_id text,
  amount decimal(18,2),
  created_at timestamp
)
distributed by (order_id);
```

#### 6.6.3 完整代码

```scala
// 导入列函数，便于明确字段清单。
import org.apache.spark.sql.functions.col

// 读取 Iceberg catalog 名称；未指定时默认 local。
val catalog = sys.env.getOrElse("ICEBERG_CATALOG", "local")
// 组织 Iceberg 源表全名。
val sourceTable = s"${catalog}.demo_db.orders_demo"
// 读取 YMatrix JDBC 地址。
val ymatrixUrl = sys.env("YMATRIX_URL")
// 读取 YMatrix 用户名。
val ymatrixUser = sys.env("YMATRIX_USER")
// 读取 YMatrix 密码。
val ymatrixPassword = sys.env("YMATRIX_PASSWORD")
// 读取目标 schema；若未设置则默认 public。
val ymatrixSchema = sys.env.getOrElse("YMATRIX_SCHEMA", "public")
// 指定目标表名；该表需要已在 YMatrix 中存在。
val targetTable = "orders_demo_sql_sink"

// 若临时 view 已存在，则先删除，避免重复创建时报错。
spark.sql("DROP VIEW IF EXISTS iceberg_orders_src")
// 若 sink view 已存在，则先删除，避免重复创建时报错。
spark.sql("DROP VIEW IF EXISTS ymatrix_orders_sink")

// 读取 Iceberg 源表，并显式列出写入所需字段。
spark.table(sourceTable)
  // 通过 select 固定列顺序，并显式处理字段类型。
  .select(
    col("order_id"),
    col("user_id"),
    col("amount").cast("decimal(18,2)").as("amount"),
    col("created_at")
  )
  // 注册为临时 view，供后续 INSERT SQL 使用。
  .createOrReplaceTempView("iceberg_orders_src")

// 创建指向 YMatrix 目标表的临时 sink view。
spark.sql(
  s"""
     |CREATE TEMPORARY VIEW ymatrix_orders_sink
     |USING com.itsumma.gpconnector.GreenplumDataSource
     |OPTIONS (
     |  url '${ymatrixUrl}',
     |  user '${ymatrixUser}',
     |  password '${ymatrixPassword}',
     |  dbschema '${ymatrixSchema}',
     |  dbtable '${targetTable}',
     |  network.timeout '120s',
     |  server.timeout '120s',
     |  dbmessages 'WARN'
     |)
     |""".stripMargin
)

// 通过 INSERT INTO TABLE 将 Iceberg 数据写入 YMatrix。
spark.sql(
  """
    |INSERT INTO TABLE ymatrix_orders_sink
    |SELECT /*+ REPARTITION(4, order_id) */
    |  order_id,
    |  user_id,
    |  amount,
    |  created_at
    |FROM iceberg_orders_src
    |""".stripMargin
)

// 读取目标表并按主键排序，校验迁移结果。
spark.read
  // 指定 connector 数据源。
  .format("its-ymatrix")
  // 指定 JDBC 地址。
  .option("url", ymatrixUrl)
  // 指定用户名。
  .option("user", ymatrixUser)
  // 指定密码。
  .option("password", ymatrixPassword)
  // 指定查询 SQL。
  .option("dbtable", s"select * from ${ymatrixSchema}.${targetTable} order by order_id")
  // 执行读取。
  .load()
  // 打印结果，不截断字段。
  .show(false)
```

#### 6.6.4 校验

```scala
// 查询目标表总行数。
spark.read
  .format("its-ymatrix")
  .option("url", sys.env("YMATRIX_URL"))
  .option("user", sys.env("YMATRIX_USER"))
  .option("password", sys.env("YMATRIX_PASSWORD"))
  .option(
    "dbtable",
    s"select count(*)::bigint as cnt from ${sys.env.getOrElse("YMATRIX_SCHEMA", "public")}.orders_demo_sql_sink"
  )
  .load()
  .show(false)
```

#### 6.6.5 注意事项

- `CREATE TEMPORARY VIEW` 对应的是临时 sink 定义，不会在 Iceberg catalog 中创建持久表
- 目标表需要预先存在，因为 provider 会从目标端推断 schema
- `INSERT INTO TABLE` 中的 `REPARTITION(4, order_id)` 控制写入前的 Spark 分区数

### 6.7 示例七：单表大数据量分批迁移

#### 6.7.1 场景说明

这个模板展示了如何按业务水位字段分批迁移单表，并支持断点续跑。

#### 6.7.2 设计思想

设计逻辑是用目标侧当前水位决定下一批起点。

逻辑如下：

```text
sourceMax = Iceberg 当前最大 ingest_id
targetMax = YMatrix 当前最大 ingest_id
nextStart = targetMax + 1

while nextStart <= sourceMax:
    取 [nextStart, nextEnd] 区间
    写入 YMatrix
    nextStart = nextEnd + 1
```

#### 6.7.3 完整代码

```scala
// 导入 JDBC DriverManager，用于直接查询目标表当前水位。
import java.sql.DriverManager
// 导入 DataFrame 类型，便于函数返回值声明。
import org.apache.spark.sql.DataFrame
// 导入 Spark 常用函数。
import org.apache.spark.sql.functions._

// 读取 Iceberg catalog 名称；未指定时默认 local。
val catalog = sys.env.getOrElse("ICEBERG_CATALOG", "local")
// 组织源表全名。
val sourceTable = s"${catalog}.test_db.iot_wide_0001"
// 指定目标表名。
val targetTable = "iot_wide_0001_batched"
// 读取 JDBC 地址。
val ymatrixUrl = sys.env("YMATRIX_URL")
// 读取用户名。
val ymatrixUser = sys.env("YMATRIX_USER")
// 读取密码。
val ymatrixPassword = sys.env("YMATRIX_PASSWORD")
// 读取目标 schema；若未设置则默认 public。
val ymatrixSchema = sys.env.getOrElse("YMATRIX_SCHEMA", "public")
// 指定每批迁移的最大行数区间。
val rowsPerBatch = 200000L

// 封装一个通用 JDBC 连接函数，便于后续多次复用。
def withConn[T](f: java.sql.Connection => T): T = {
  // 建立到 YMatrix 的 JDBC 连接。
  val conn = DriverManager.getConnection(ymatrixUrl, ymatrixUser, ymatrixPassword)
  // 在连接作用域内执行传入逻辑。
  try f(conn)
  // 无论成功失败都关闭连接，避免泄漏。
  finally conn.close()
}

// 判断目标表是否已经存在。
def targetTableExists(): Boolean = {
  // 在一个临时 JDBC 连接中执行元数据查询。
  withConn { conn =>
    // 通过 pg_catalog.pg_tables 查询目标表是否存在。
    val stmt = conn.prepareStatement(
      """
        |select count(*)::bigint
        |from pg_catalog.pg_tables
        |where schemaname = ?
        |  and tablename = ?
        |""".stripMargin
    )
    try {
      // 绑定 schema 参数。
      stmt.setString(1, ymatrixSchema)
      // 绑定表名参数。
      stmt.setString(2, targetTable)
      // 执行查询。
      val rs = stmt.executeQuery()
      try {
        // 如果返回计数大于 0，则说明目标表存在。
        rs.next() && rs.getLong(1) > 0L
      } finally {
        // 关闭结果集。
        rs.close()
      }
    } finally {
      // 关闭 PreparedStatement。
      stmt.close()
    }
  }
}

// 查询目标表当前已同步到的最大 ingest_id。
def currentTargetMax(): Long = {
  // 若目标表尚不存在，则视为还未同步过任何数据。
  if (!targetTableExists()) {
    0L
  } else {
    // 若目标表存在，则通过 JDBC 查询最大水位。
    withConn { conn =>
      // 构造查询目标表最大 ingest_id 的 SQL。
      val stmt = conn.prepareStatement(
        s"select coalesce(max(ingest_id), 0)::bigint from ${ymatrixSchema}.${targetTable}"
      )
      try {
        // 执行查询。
        val rs = stmt.executeQuery()
        try {
          // 如果结果存在，则返回最大值；否则返回 0。
          if (rs.next()) rs.getLong(1) else 0L
        } finally {
          // 关闭结果集。
          rs.close()
        }
      } finally {
        // 关闭 PreparedStatement。
        stmt.close()
      }
    }
  }
}

// 查询 Iceberg 源表当前最大 ingest_id。
def sourceMax(): Long = {
  // 读取源表后聚合出最大 ingest_id；若表为空则返回 0。
  spark.table(sourceTable)
    // 用 coalesce 处理空表情况。
    .agg(coalesce(max(col("ingest_id")), lit(0L)))
    // 将聚合结果转为 Long Dataset。
    .as[Long]
    // 直接取出单条结果。
    .head()
}

// 按指定 ingest_id 区间构造一个待写入批次。
def buildBatch(startId: Long, endId: Long): DataFrame = {
  // 从源表读取数据。
  spark.table(sourceTable)
    // 只保留当前批次对应的 ingest_id 区间。
    .where(col("ingest_id").between(startId, endId))
    // 对当前批次重分区，控制写入并发。
    .repartition(4)
}

// 获取源表当前最大水位。
val maxSourceId = sourceMax()
// 计算下一批应从哪里开始；若目标表不存在，则从 1 开始附近写入。
var nextStartId = currentTargetMax() + 1L

// 当下一批起始水位不超过源表最大水位时，循环迁移。
while (nextStartId <= maxSourceId) {
  // 计算当前批次的结束水位，避免超过源表最大值。
  val nextEndId = math.min(nextStartId + rowsPerBatch - 1L, maxSourceId)
  // 构造当前批次数据。
  val batchDf = buildBatch(nextStartId, nextEndId)

  // 将当前批次写入 YMatrix。
  batchDf.write
    // 指定 connector 数据源。
    .format("its-ymatrix")
    // 指定 JDBC 地址。
    .option("url", ymatrixUrl)
    // 指定用户名。
    .option("user", ymatrixUser)
    // 指定密码。
    .option("password", ymatrixPassword)
    // 指定目标 schema。
    .option("dbschema", ymatrixSchema)
    // 指定目标表名。
    .option("dbtable", targetTable)
    // 指定目标分布键。
    .option("distributedby", "ingest_id")
    // 设置网络超时。
    .option("network.timeout", "120s")
    // 设置服务端超时。
    .option("server.timeout", "120s")
    // 采用追加模式写入批次数据。
    .mode("append")
    // 真正执行写入。
    .save()

  // 输出当前批次已完成的 ingest_id 范围，便于日志追踪。
  println(s"copied ingest_id [$nextStartId, $nextEndId]")
  // 将下一批起始位置推进到当前批次结束位置之后。
  nextStartId = nextEndId + 1L
}
```

#### 6.7.4 使用场景

- 单表亿级数据迁移
- 需要支持重跑
- 需要明确批次边界
- 需要记录每批写入范围

### 6.8 示例八：在 Spark 内直接校验 YMatrix

#### 6.8.1 场景说明

开发联调时可以直接在 Spark 内回查 YMatrix 结果。

#### 6.8.2 行数校验

```scala
// 创建一个读取入口，并准备通过 connector 查询 YMatrix 行数。
spark.read
  // 指定 connector 数据源。
  .format("its-ymatrix")
  // 指定 JDBC 地址。
  .option("url", sys.env("YMATRIX_URL"))
  // 指定用户名。
  .option("user", sys.env("YMATRIX_USER"))
  // 指定密码。
  .option("password", sys.env("YMATRIX_PASSWORD"))
  // 指定统计目标表行数的 SQL。
  .option(
    "dbtable",
    s"select count(*)::bigint as cnt from ${sys.env.getOrElse("YMATRIX_SCHEMA", "public")}.orders_demo"
  )
  // 执行读取。
  .load()
  // 打印结果且不截断。
  .show(false)
```

#### 6.8.3 水位校验

```scala
// 创建一个读取入口，并准备查询目标表的最小和最大 order_id。
spark.read
  // 指定 connector 数据源。
  .format("its-ymatrix")
  // 指定 JDBC 地址。
  .option("url", sys.env("YMATRIX_URL"))
  // 指定用户名。
  .option("user", sys.env("YMATRIX_USER"))
  // 指定密码。
  .option("password", sys.env("YMATRIX_PASSWORD"))
  // 指定查询目标表水位范围的 SQL。
  .option(
    "dbtable",
    s"select min(order_id) as min_id, max(order_id) as max_id from ${sys.env.getOrElse("YMATRIX_SCHEMA", "public")}.orders_demo"
  )
  // 执行读取。
  .load()
  // 打印结果。
  .show(false)
```

#### 6.8.4 抽样校验

```scala
// 创建一个读取入口，并准备抽样查看目标表的前 10 行数据。
spark.read
  // 指定 connector 数据源。
  .format("its-ymatrix")
  // 指定 JDBC 地址。
  .option("url", sys.env("YMATRIX_URL"))
  // 指定用户名。
  .option("user", sys.env("YMATRIX_USER"))
  // 指定密码。
  .option("password", sys.env("YMATRIX_PASSWORD"))
  // 指定抽样查询 SQL。
  .option(
    "dbtable",
    s"select * from ${sys.env.getOrElse("YMATRIX_SCHEMA", "public")}.orders_demo order by order_id limit 10"
  )
  // 执行读取。
  .load()
  // 打印抽样结果。
  .show(false)
```

## 7. 深入原理与性能调优

### 7.1 为什么写入性能不只取决于 Spark

开发人员常见误区是把性能问题全部归因于 Spark 分区数。实际上，写入性能至少受四类因素共同影响：

| 因素 | 影响内容 |
|---|---|
| Spark 并发 | 任务数量、分区数量、shuffle 行为 |
| 网络 | Spark Worker 与 YMatrix segment 间的传输质量 |
| YMatrix 分布 | 目标表分布键是否合理 |
| 数据形态 | 宽表/窄表、行大小、字段类型 |

### 7.2 `repartition` 如何理解

`repartition` 用于控制 Spark 在写入前如何组织并发与数据切分。

使用方式：

- 小规模验证时先从 `4` 或 `8` 开始
- 与 YMatrix primary segment 数量协调
- 避免远大于 segment 数量的无意义放大

### 7.3 批大小如何理解

`rowsPerBatch` 不是越大越好。

较大批次的优点：

- 减少批次管理开销
- 提高大批量吞吐

较大批次的风险：

- 失败重跑成本高
- 单批持续时间长
- 网络与资源抖动放大

批大小选择参考：

| 阶段 | 参考范围 |
|---|---|
| 首次联调 | `10000 ~ 100000` |
| 中等规模验证 | `100000 ~ 500000` |
| 正式迁移 | 结合网络与资源压测逐步放大 |

### 7.4 何时用 `append`

`append` 适用场景：

- 分批迁移
- 增量同步
- 目标表已存在并持续追加

但前提是：

- 有明确的幂等或水位策略

### 7.5 何时用 `overwrite`

`overwrite` 适用场景：

- 测试表反复重灌
- 首次开发联调
- 需要明确清空旧数据

但要注意：

- 如果结构变化明显，`overwrite` 可能带来结构重建行为

### 7.6 目标表分布键如何选

分布键选择顺序：

1. 高基数
2. 稳定
3. 常用于查询或 join
4. 尽量避免热点

对于本文中的宽表示例，`ingest_id` 可作为压测和迁移验证场景下的默认分布键。

### 7.7 常见性能误区

常见误区包括：

- 误以为把 `repartition` 调到很大就一定更快
- 误以为 `append` 天然幂等
- 误以为目标表自动建表后一定满足生产要求
- 误以为网络超时就是 JDBC 配置问题
- 误以为只要 Driver 能连库，整个链路就没问题

## 8. 高级功能与扩展方式

### 8.1 目标表自动创建与预建表

两种方式都可以使用：

| 方式 | 优点 | 风险 |
|---|---|---|
| 自动建表 | 开发联调快 | DDL 可控性较弱 |
| 预建表 | 结构更可控 | 前置工作更多 |

开发阶段与生产阶段可采用不同表创建方式：

- 联调阶段可使用自动建表
- 正式环境通常预建关键业务表

### 8.2 目标表结构映射的控制点

目标表结构映射的主要控制点有三类：

1. `select(...)` 明确列清单
2. `cast(...)` 明确类型
3. 预建目标表明确 DDL

### 8.3 从脚本演进到正式作业

代码分层可以按以下方式组织：

```text
loadIcebergTable()
        |
transform()
        |
writeToYMatrix()
        |
validate()
```

代码模板：

```scala
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

def loadIcebergTable(fullTableName: String): DataFrame = {
  spark.table(fullTableName)
}

def transform(df: DataFrame): DataFrame = {
  df.select(
    col("ingest_id"),
    col("event_id"),
    col("event_time"),
    col("device_id")
  )
}

def writeToYMatrix(df: DataFrame, tableName: String): Unit = {
  df.write
    .format("its-ymatrix")
    .option("url", sys.env("YMATRIX_URL"))
    .option("user", sys.env("YMATRIX_USER"))
    .option("password", sys.env("YMATRIX_PASSWORD"))
    .option("dbschema", sys.env.getOrElse("YMATRIX_SCHEMA", "public"))
    .option("dbtable", tableName)
    .option("distributedby", "ingest_id")
    .option("network.timeout", "120s")
    .option("server.timeout", "120s")
    .mode("append")
    .save()
}

def validate(tableName: String): Unit = {
  spark.read
    .format("its-ymatrix")
    .option("url", sys.env("YMATRIX_URL"))
    .option("user", sys.env("YMATRIX_USER"))
    .option("password", sys.env("YMATRIX_PASSWORD"))
    .option(
      "dbtable",
      s"select count(*)::bigint as cnt from ${sys.env.getOrElse("YMATRIX_SCHEMA", "public")}.$tableName"
    )
    .load()
    .show(false)
}

val catalog = sys.env.getOrElse("ICEBERG_CATALOG", "local")
val src = loadIcebergTable(s"${catalog}.test_db.iot_wide_0001")
val dst = transform(src)
writeToYMatrix(dst, "iot_wide_for_app")
validate("iot_wide_for_app")
```
