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

### 1.1 它是什么

本项目是一个 Spark DataSource V2 Connector。对于开发人员而言，它的本质不是一个单独的“迁移程序”，而是一个可以直接嵌入 Spark 作业中的数据读写组件。

在 Iceberg 到 YMatrix 的场景中，标准开发链路如下：

1. Spark 读取 Iceberg 表
2. Spark 对 DataFrame 做字段选择、过滤、转换、重分区
3. Spark 通过 connector 将 DataFrame 写入 YMatrix

开发接入时的核心入口只有一个：

```scala
.format("its-ymatrix")
```

也就是说，目标虽然是 YMatrix，但当前仓库中的接入格式、参数模型和读写方式，都统一按 YMatrix 接口来使用。

### 1.2 它解决什么问题

在开发实践中，Iceberg 到 YMatrix 的迁移通常会遇到以下问题：

- 需要在 Spark 内部直接完成离线迁移，不希望额外引入独立同步系统
- 需要按业务字段进行过滤、重命名、映射和类型转换
- 需要可控地按批写入，以支持大表迁移和断点续跑
- 需要将迁移逻辑写成标准 Spark 作业，而不是临时脚本堆砌
- 需要在 Spark 内直接完成结果校验与联调

本 connector 适合解决的正是上述“开发内嵌式迁移”问题。

### 1.3 它不是什么

为了避免误用，需要先明确边界。

本 connector 不是：

- 自动 CDC 平台
- 自动元数据同步平台
- 自动表结构演进治理平台
- 自动去重补偿平台
- 自动增量语义保障平台

也就是说，字段映射、幂等控制、水位推进、重跑策略、目标表治理，仍然需要由开发代码或外围流程来承担。

## 2. 核心定位与典型场景

### 2.1 核心定位

从架构定位上看，这个 connector 更像一个“Spark 到 YMatrix 的高性能写入组件”，而不是一套封闭的迁移产品。

开发人员可以把它理解成三层结构中的最底层一层：

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

| 场景 | 是否适合 | 说明 |
|---|---|---|
| 单表一次性迁移 | 适合 | 适合开发验证和小规模上线 |
| 多表批量回灌 | 适合 | 可配合脚本按表循环执行 |
| 大表按水位字段分批迁移 | 适合 | 推荐采用 `ingest_id`、`order_id` 等单调字段 |
| 增量同步演示 | 适合 | 可基于业务水位做简化同步 |
| 强一致 CDC | 不适合直接承担 | 需要外围机制配合 |
| 自动 DDL 治理 | 不适合直接承担 | 需要开发侧自行约束 |

### 2.3 推荐使用方式

推荐做法：

1. 先用最小 Demo 验证环境和权限
2. 再用单表迁移模板验证字段映射和类型兼容
3. 再上分批迁移或多表迁移
4. 最后再做增量同步或持续运行任务

不推荐一开始直接把全部业务表一次性迁移到正式环境。

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

这意味着在开发阶段可以降低初次接入门槛，但在生产环境中仍建议预先规划 schema，而不是完全依赖自动创建。

#### 3.2.2 `dbtable`

`dbtable` 在写入时表示目标表名；在读取时既可以是目标表名，也可以是一段 SQL 查询。

这使得开发人员可以直接在 Spark 中用 connector 反查 YMatrix 的结果，而不必每次切换到 `psql`。

#### 3.2.3 `distributedby`

`distributedby` 影响目标表在 YMatrix 中的分布方式。它不是普通参数，而是直接决定后续数据分布、倾斜情况和部分查询性能的关键参数。

推荐原则：

- 优先选高基数字段
- 优先选写入与查询都较稳定的字段
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

### 4.2 为什么网络要求严格

当前 connector 使用 GPFDIST 协议做数据传输。它不是简单的“Driver 通过 JDBC 一条条写入”，而是由 Spark 侧和 YMatrix 侧协同完成更高吞吐的数据交换。

因此，网络要求比普通 JDBC 写入更严格：

- 不是只有 Driver 能连 YMatrix 就够了
- YMatrix segment 也必须能够反向访问 Spark Worker 暴露的数据服务

如果这一点不满足，开发时会看到的现象通常不是“SQL 错误”，而是：

- 任务长时间等待
- 连接超时
- GPFDIST 无法建立会话

### 4.3 建议的统一环境变量

为了让本文中的所有 Demo 尽量可直接复现，建议先统一环境变量：

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

### 4.4 推荐的 `spark-shell` 启动模板

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

### 4.5 启动后的第一轮自检

进入 `spark-shell` 后，推荐先执行以下命令：

```scala
val catalog = sys.env.getOrElse("ICEBERG_CATALOG", "local")
spark.sql("show databases").show(false)
println(sys.env.getOrElse("YMATRIX_URL", "YMATRIX_URL_NOT_SET"))
println(s"catalog = $catalog")
```

如果这里已经失败，说明问题还在环境层，而不是迁移逻辑层。

## 5. 快速开始

### 5.1 最小接入代码模型

开发时最重要的不是记住所有参数，而是先记住最小代码模型：

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

这个模型表达了整个开发本质：

- 源侧是 Spark DataFrame
- 目标侧是 `its-ymatrix` sink
- 迁移逻辑发生在 Spark 中

### 5.2 开发接入的最小参数集

| 参数 | 是否建议必填 | 说明 |
|---|---|---|
| `url` | 是 | YMatrix JDBC 地址 |
| `user` | 是 | 用户名 |
| `password` | 是 | 密码 |
| `dbschema` | 是 | 目标 schema |
| `dbtable` | 是 | 目标表 |
| `mode` | 是 | 建议明确为 `append` 或 `overwrite` |
| `distributedby` | 强烈建议 | 自动建表时指定分布键 |
| `network.timeout` | 建议 | 网络敏感环境建议显式设定 |
| `server.timeout` | 建议 | 大批量传输建议显式设定 |

### 5.3 第一优先级的开发原则

第一优先级不是“先把命令写长”，而是先保证三件事：

1. 源表字段清单明确
2. 目标表写入模式明确
3. 水位与幂等策略明确

## 6. 开发实战与完整示例

本节是本文重点。所有示例都按“场景说明、完整代码、结果校验、注意事项”展开。
为便于新人直接照着理解和改造，下面所有 Demo 代码均采用逐行注释方式编写。

### 6.1 示例一：最小可复现链路

#### 6.1.1 场景说明

这是第一次联调时最推荐执行的示例。它验证的是最小闭环：

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

- 第一次联调优先用 `overwrite`
- 目标表若已存在旧结构，可能影响结果
- 如需固定网络排查，优先显式指定 `server.port`

### 6.2 示例二：迁移已有 Iceberg 表

#### 6.2.1 场景说明

这是最接近真实业务开发的基础模板。它不负责造数据，而是直接迁移已经存在的 Iceberg 表。

#### 6.2.2 设计原则

推荐先解释为什么不建议直接 `select("*")`：

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

#### 6.2.4 推荐做法

- 对宽表显式维护字段列表
- `repartition` 与 Spark 并发、YMatrix segment 数量一起评估
- 初次迁移建议先跑小分区、小样本

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
  // 只保留指定主键范围内的数据，适合做灰度验证。
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

#### 6.4.3 推荐做法

推荐做法如下：

- 字段映射逻辑集中在一处维护
- 对关键类型显式 `cast`
- 对金额、时间、主键字段优先做严格映射

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
  // 告诉 connector 在 overwrite 时优先走 truncate，尽量保留表结构。
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
| `mode("overwrite") + truncate=true` | 优先尝试保留表结构、仅清空数据 |

### 6.6 示例六：先造 Iceberg 测试数据，再做批量迁移

#### 6.6.1 场景说明

这是当前仓库最适合开发联调的完整链路：

1. 用仓库脚本生成 Iceberg 测试宽表
2. 再用仓库脚本批量迁移到 YMatrix

#### 6.6.2 第一步：造数

在 `spark-shell` 中执行：

```scala
// 加载仓库内置的 Iceberg 宽表造数脚本。
:load examples/iceberg-wide-table-bulk-load.scala
```

脚本默认会：

- 创建 `local.test_db`
- 创建 `iot_wide_0001` 等宽表
- 向每张表分批写入测试数据
- 基于 `ingest_id` 支持续跑

建议第一次先使用脚本默认小规模参数，不要直接放大到极限规模。

#### 6.6.3 第二步：批量迁移

```scala
// 加载仓库内置的 Iceberg 到 YMatrix 批量迁移脚本。
:load examples/iceberg-to-ymatrix-bulk-load.scala
```

这个脚本会：

- 逐张表读取 Iceberg 宽表
- 检查 YMatrix 目标表是否存在
- 自动根据 `max(ingest_id)` 判断是否需要继续迁移
- 每次按 `rowsPerBatch` 分段写入

#### 6.6.4 推荐修改参数

在正式执行前，优先调整以下参数：

```scala
// 指定 Iceberg 源 namespace。
val icebergNamespace = "local.test_db"
// 指定 YMatrix JDBC 地址。
val ymatrixUrl = "jdbc:postgresql://ymatrix-master-host:5432/your_database"
// 指定 YMatrix 用户名。
val ymatrixUser = "database_user"
// 指定 YMatrix 密码。
val ymatrixPassword = "yourpassword"
// 指定目标 schema。
val ymatrixSchema = "public"
// 从第 1 张表开始迁移。
val tableStartIndex = 1
// 本次只迁移 3 张表，适合联调。
val tableCount = 3
// 每批迁移 20 万行。
val rowsPerBatch = 200000L
// 写入前使用 8 个分区。
val writePartitions = 8
// 指定目标分布键。
val ymatrixDistributedBy = "ingest_id"
```

#### 6.6.5 推荐校验

```scala
// 查看 namespace 下有哪些 Iceberg 表。
spark.sql("show tables in local.test_db").show(50, false)
// 校验第一张宽表总行数。
spark.sql("select count(*) from local.test_db.iot_wide_0001").show(false)
// 校验第一张宽表的 ingest_id 范围。
spark.sql("select min(ingest_id), max(ingest_id) from local.test_db.iot_wide_0001").show(false)
```

### 6.7 示例七：单表大数据量分批迁移

#### 6.7.1 场景说明

这是本文最重要的业务模板之一。它展示了如何把一个大表按业务水位字段分批迁移，并支持断点续跑。

#### 6.7.2 设计思想

设计本质不是“循环写入”，而是“用目标侧当前水位去决定下一批该从哪里开始”。

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

#### 6.7.4 推荐使用场景

- 单表亿级数据迁移
- 需要支持重跑
- 需要明确批次边界
- 需要记录每批写入范围

### 6.8 示例八：多表批量迁移

#### 6.8.1 场景说明

多表批量迁移不应一开始就写成复杂框架。当前仓库已经提供了足够清晰的脚本模板，可直接复用。

#### 6.8.2 执行方式

```scala
// 加载仓库内置的多表批量迁移脚本。
:load examples/iceberg-to-ymatrix-bulk-load.scala
```

#### 6.8.3 标准参数解释

| 参数 | 含义 |
|---|---|
| `tableStartIndex` | 从第几张表开始 |
| `tableCount` | 本次迁移多少张表 |
| `rowsPerBatch` | 每批迁移多少行区间 |
| `writePartitions` | 写入前的 Spark 分区数 |
| `ymatrixDistributedBy` | 目标分布键 |

#### 6.8.4 常见执行方式

只迁移前 3 张表：

```scala
// 从第 1 张表开始迁移。
val tableStartIndex = 1
// 本次总共迁移 3 张表。
val tableCount = 3
```

从第 21 张表开始迁移 5 张表：

```scala
// 从第 21 张表开始迁移。
val tableStartIndex = 21
// 本次总共迁移 5 张表。
val tableCount = 5
```

### 6.9 示例九：增量同步演示

#### 6.9.1 场景说明

当团队需要先理解“持续同步”的基本思路时，最好的方式不是直接讨论复杂 CDC，而是先看一个水位驱动的简单循环模型。

当前仓库已经提供了对应示例：

```scala
// 加载仓库内置的增量同步演示脚本。
:load examples/iceberg-to-ymatrix-continuous-sync.scala
```

#### 6.9.2 运行前准备

先在 YMatrix 中创建目标表：

```sql
-- 创建增量同步演示用的目标表。
create table public.iceberg_sync_demo (
  -- 订单主键，用于作为同步水位字段。
  order_id bigint,
  -- 用户标识字段。
  user_id text,
  -- 金额字段，保留两位小数。
  amount decimal(18,2),
  -- 订单创建时间。
  created_at timestamp,
  -- 批次号，用于标识每轮演示写入。
  batch_id bigint
-- 指定目标表分布键。
)
-- 以 order_id 作为分布键创建表。
distributed by (order_id);
```

#### 6.9.3 示例本质

这个示例的本质是：

1. 向 Iceberg 持续追加新数据
2. 每轮读取目标侧当前最大 `order_id`
3. 只同步尚未写入目标侧的新行

它适合理解增量思路，但不应直接等同于生产级 CDC。

### 6.10 示例十：在 Spark 内直接校验 YMatrix

#### 6.10.1 场景说明

开发联调过程中，最常见低效动作是频繁在 `spark-shell`、`psql`、日志之间来回切换。更推荐的方式是直接在 Spark 内做结果回查。

#### 6.10.2 行数校验

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

#### 6.10.3 水位校验

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

#### 6.10.4 抽样校验

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

`repartition` 的本质不是“数字越大越快”，而是控制 Spark 在写入前如何组织并发与数据切分。

推荐做法：

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

推荐策略：

| 阶段 | 建议 |
|---|---|
| 首次联调 | `10000 ~ 100000` |
| 中等规模验证 | `100000 ~ 500000` |
| 正式迁移 | 结合网络与资源压测逐步放大 |

### 7.4 何时用 `append`

推荐使用 `append` 的场景：

- 分批迁移
- 增量同步
- 目标表已存在并持续追加

但前提是：

- 有明确的幂等或水位策略

### 7.5 何时用 `overwrite`

推荐使用 `overwrite` 的场景：

- 测试表反复重灌
- 首次开发联调
- 需要明确清空旧数据

但要注意：

- 如果结构变化明显，`overwrite` 可能带来结构重建行为

### 7.6 目标表分布键如何选

优先级建议：

1. 高基数
2. 稳定
3. 常用于查询或 join
4. 尽量避免热点

对于本文中的宽表示例，`ingest_id` 适合作为压测和迁移验证场景下的默认分布键。

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

开发建议：

- 联调阶段优先自动建表
- 正式环境建议关键业务表预建表

### 8.2 目标表结构映射的控制点

开发中最重要的控制点有三类：

1. `select(...)` 明确列清单
2. `cast(...)` 明确类型
3. 预建目标表明确 DDL

### 8.3 从脚本演进到正式作业

推荐的代码分层方式如下：

```text
loadIcebergTable()
        |
transform()
        |
writeToYMatrix()
        |
validate()
```

推荐的代码模板：

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

## 9. 安全、权限与常见误区

### 9.1 不要在正式代码中硬编码密码

仓库中的示例脚本为了演示方便，存在直接写密码的示例。开发手册中的标准建议是：

- 使用环境变量
- 使用 Spark 配置
- 使用密钥管理系统

### 9.2 权限建议

开发环境与正式环境建议区分：

| 环境 | 建议 |
|---|---|
| 开发联调 | 可适当放宽，优先打通链路 |
| 测试环境 | 开始收敛 schema 与建表权限 |
| 正式环境 | 按最小权限原则控制 |

### 9.3 常见误区汇总

| 误区 | 正确认识 |
|---|---|
| connector 会自动保证幂等 | 幂等取决于你的业务设计 |
| 可以直接依赖 `select *` | 宽表迁移建议显式列清单 |
| `append` 就等于增量同步 | 增量同步还需要水位控制 |
| 自动建表即可替代建模 | 自动建表适合联调，不等于最终治理 |
| 只有 Spark 能连库就够了 | 还需要 YMatrix segment 到 Spark Worker 可达 |

## 10. 附录

### 10.1 建议的开发验证顺序

推荐按以下顺序开展开发：

步骤一：跑“最小可复现链路”

步骤二：在 Spark 内反查 YMatrix，确认结果无误

步骤三：迁移已有 Iceberg 表，确认字段和类型映射

步骤四：做范围迁移或小批量分批迁移

步骤五：跑仓库现成的多表脚本

步骤六：把脚本提炼成正式作业代码

### 10.2 最低校验清单

每次迁移完成后，至少检查以下内容：

1. 源表行数
2. 目标表行数
3. 水位字段最小值和最大值
4. 抽样数据内容
5. 目标表 schema 是否符合预期

### 10.3 相关仓库文件

建议结合以下文件一起阅读：

- `README.md`
- `docs/iceberg-ymatrix-test-plan.md`
- `examples/iceberg-wide-table-bulk-load.scala`
- `examples/iceberg-to-ymatrix-bulk-load.scala`
- `examples/iceberg-to-ymatrix-continuous-sync.scala`

### 10.4 一句话结论

对于开发人员而言，Iceberg 到 YMatrix 的标准做法并不复杂，其本质就是：

先在 Spark 中把 Iceberg 数据组织成正确的 DataFrame，再通过 `format("its-ymatrix")` 以明确的写入模式、分布键和水位策略写入 YMatrix。

真正决定项目质量的，不是 connector 调用这一行代码本身，而是你如何设计字段映射、分批策略、幂等控制、校验方法和上线方式。
