# 从 Spark 写入 YMatrix
## 基于 `spark-ymatrix-connector` 的数据导入指南

| 项 | 说明 |
|---|---|
| 章节位置 | 使用数据库 -> 数据写入 -> 从 Spark 写入 |
| 覆盖范围 | 使用 `spark-ymatrix-connector` 将 Spark 中的数据写入 YMatrix |
| 文档类型 | 使用指南 |
| 适用对象 | Spark 开发人员、数据平台开发人员、数据集成开发人员 |
| 版本基线 | Spark 3.4.1 / Scala 2.12.17 / Java 1.8 / Connector 3.1.0 |
| 代码基线 | 当前仓库 `spark-ymatrix-connector` |
| 文档日期 | 2026-03-23 |

## 目录

1. [概述](#1-概述)
2. [适用范围与边界](#2-适用范围与边界)
3. [先认识最小写入代码](#3-先认识最小写入代码)
4. [jar 包导入与使用方式](#4-jar-包导入与使用方式)
5. [开发示例](#5-开发示例)
6. [常见问题与注意事项](#6-常见问题与注意事项)

## 1. 概述

`spark-ymatrix-connector` 是一个 Spark DataSource V2 Connector，用来把 Spark 中的数据写入 YMatrix。

你真正需要记住的入口只有一个：

```scala
.format("its-ymatrix")
```

只要数据已经进入 Spark，并且可以表示为 `DataFrame`、Spark 表或临时视图，就可以通过这个 connector 写入 YMatrix。

典型来源包括：

- Spark SQL 表
- `parquet`、`orc`、`csv`、`json` 等文件数据
- 通过 JDBC 读入 Spark 的数据
- Hive、Iceberg、Delta 等上游表格式
- 程序中直接构造的测试数据

本文不把重点放在抽象概念上，而是直接教你两件事：

1. 怎么把 `jar` 包带进 Spark
2. 怎么真正把数据写进 YMatrix

## 2. 适用范围

### 2.1 适用范围

本 connector 适合以下场景：

| 场景 | 支持情况 | 说明 |
|---|---|---|
| Spark DataFrame 写入 YMatrix | 支持 | 最常见的离线导入方式 |
| Spark 表到 YMatrix 单表导入 | 支持 | 适合从 Spark SQL 表或临时视图导入 |
| 写入前做过滤、字段映射、重命名 | 支持 | 在 Spark 内完成转换后再写入 |
| 按范围分批导入大表 | 支持 | 可基于业务主键或水位字段分批推进 |
| Spark SQL 方式写入 | 支持 | 通过临时 sink view + `INSERT INTO` 实现 |
| 写后回读校验 | 支持 | 可直接通过 connector 从 YMatrix 回读结果 |


## 3. 先认识最小写入代码

先看最小可用模型：

```scala
spark.table("db.source_table")
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

这段代码表达的意思很简单：

1. 从 Spark 里拿到一份数据
2. 指定写入目标是 `its-ymatrix`
3. 告诉 connector 目标库地址、账号、schema 和表名
4. 指定写入模式
5. 执行 `.save()`

最常用参数如下：

| 参数 | 是否必填 | 说明 |
|---|---|---|
| `url` | 是 | YMatrix JDBC 地址 |
| `user` | 是 | 用户名 |
| `password` | 是 | 密码 |
| `dbschema` | 建议填写 | 目标 schema |
| `dbtable` | 是 | 目标表名 |
| `mode` | 是 | 必须显式指定 `append` 或 `overwrite` |
| `distributedby` | 自动建表时建议填写 | 自动建表时指定分布键 |
| `network.timeout` | 否 | 网络敏感环境建议显式设置 |
| `server.timeout` | 否 | 大批量传输建议显式设置 |
| `server.port` | 否 | 联调网络问题时可固定端口 |
| `truncate` | 否 | 配合 `overwrite` 使用，尽量保留表结构 |

## 4. jar 包导入与使用方式

怎么把 connector 的 `jar` 包正确带进 Spark。

当前仓库里已经构建出的 `jar` 路径是：

```text
/root/spark-greenplum-connector/spark-greenplum-connector/target/spark-ymatrix-connector_2.12-3.1.jar
```

下面分两种方式讲。

### 4.1 方式一：程序调用

这种方式适合你已经有正式 Spark 作业代码，需要把 connector 集成进程序。

#### 第一步：准备 jar 包

确认 `jar` 文件存在：

```bash
ls -lh /root/spark-greenplum-connector/spark-greenplum-connector/target/spark-ymatrix-connector_2.12-3.1.jar
```

#### 第二步：编写 Spark 程序

下面是一份最小可运行的 Spark 应用示例：

```scala
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object SparkWriteToYMatrixApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("SparkWriteToYMatrixApp")
      .getOrCreate()

    val srcDf = spark.table("dm.orders_daily")
      .select(
        col("order_id"),
        col("user_id"),
        col("pay_amount").cast("decimal(18,2)").as("pay_amount"),
        col("created_at").cast("timestamp").as("created_at")
      )
      .repartition(4, col("order_id"))

    srcDf.write
      .format("its-ymatrix")
      .option("url", "jdbc:postgresql://ymatrix-master-host:5432/your_database")
      .option("user", "database_user")
      .option("password", "yourpassword")
      .option("dbschema", "public")
      .option("dbtable", "orders_daily")
      .option("distributedby", "order_id")
      .option("network.timeout", "120s")
      .option("server.timeout", "120s")
      .mode("append")
      .save()

    spark.stop()
  }
}
```

#### 第三步：提交程序时带上 jar

如果你的业务作业本身已经打成一个单独的应用包，提交时把 connector 用 `--jars` 带上：

```bash
export CONNECTOR_JAR=/root/spark-greenplum-connector/spark-greenplum-connector/target/spark-ymatrix-connector_2.12-3.1.jar
export APP_JAR=/path/to/your-spark-app.jar
export SPARK_LOCAL_IP=172.16.100.32

spark-submit \
  --class SparkWriteToYMatrixApp \
  --master local[4] \
  --driver-memory 8g \
  --conf spark.driver.host=${SPARK_LOCAL_IP} \
  --conf spark.driver.bindAddress=0.0.0.0 \
  --conf spark.local.ip=${SPARK_LOCAL_IP} \
  --conf spark.sql.shuffle.partitions=4 \
  --jars ${CONNECTOR_JAR} \
  ${APP_JAR}
```

#### 第四步：怎么看是否接入成功

至少看这三点：

1. 程序没有报 `Failed to find data source: its-ymatrix`
2. `.save()` 阶段没有报连接或 `gpfdist` 错误
3. 写完后能从 YMatrix 查到数据

### 4.2 方式二：命令行调用

这种方式适合联调、排障、Demo 演示和一次性导入任务。

命令行调用最常见有两种：

- `spark-shell`
- `spark-submit`

#### 4.2.1 用 `spark-shell` 直接写入

这是最适合第一次验证链路的方式。

先启动 `spark-shell`：

```bash
export CONNECTOR_JAR=/root/spark-greenplum-connector/spark-greenplum-connector/target/spark-ymatrix-connector_2.12-3.1.jar
export YMATRIX_URL=jdbc:postgresql://ymatrix-master-host:5432/your_database
export YMATRIX_USER=database_user
export YMATRIX_PASSWORD=yourpassword
export YMATRIX_SCHEMA=public
export SPARK_LOCAL_IP=172.16.100.32

spark-shell \
  --master local[4] \
  --driver-memory 8g \
  --conf spark.driver.host=${SPARK_LOCAL_IP} \
  --conf spark.driver.bindAddress=0.0.0.0 \
  --conf spark.local.ip=${SPARK_LOCAL_IP} \
  --conf spark.sql.shuffle.partitions=4 \
  --jars ${CONNECTOR_JAR}
```

进入 `spark-shell` 后，直接执行：

```scala
import spark.implicits._
import org.apache.spark.sql.functions._

val srcDf = Seq(
  (1L, "u001", BigDecimal("18.50"), "2026-03-23 10:00:00"),
  (2L, "u002", BigDecimal("20.00"), "2026-03-23 10:05:00"),
  (3L, "u003", BigDecimal("99.99"), "2026-03-23 10:10:00")
).toDF("order_id", "user_id", "amount", "created_at")
  .withColumn("created_at", col("created_at").cast("timestamp"))

srcDf.write
  .format("its-ymatrix")
  .option("url", sys.env("YMATRIX_URL"))
  .option("user", sys.env("YMATRIX_USER"))
  .option("password", sys.env("YMATRIX_PASSWORD"))
  .option("dbschema", sys.env.getOrElse("YMATRIX_SCHEMA", "public"))
  .option("dbtable", "orders_demo")
  .option("distributedby", "order_id")
  .option("server.port", "43000")
  .mode("overwrite")
  .save()
```

然后马上做一次回读校验：

```scala
spark.read
  .format("its-ymatrix")
  .option("url", sys.env("YMATRIX_URL"))
  .option("user", sys.env("YMATRIX_USER"))
  .option("password", sys.env("YMATRIX_PASSWORD"))
  .option(
    "dbtable",
    s"select * from ${sys.env.getOrElse("YMATRIX_SCHEMA", "public")}.orders_demo order by order_id"
  )
  .load()
  .show(false)
```

#### 4.2.2 用 `spark-submit` 跑脚本或程序

如果你不想手工进交互式 shell，可以把逻辑写进程序后直接 `spark-submit`。

命令形式和“程序调用”本质一样，关键点仍然是：

- 业务程序是你的应用包
- connector 是通过 `--jars` 带进去的外部依赖

最小命令模板如下：

```bash
spark-submit \
  --class SparkWriteToYMatrixApp \
  --master local[4] \
  --driver-memory 8g \
  --conf spark.driver.host=${SPARK_LOCAL_IP} \
  --conf spark.driver.bindAddress=0.0.0.0 \
  --conf spark.local.ip=${SPARK_LOCAL_IP} \
  --jars /root/spark-greenplum-connector/spark-greenplum-connector/target/spark-ymatrix-connector_2.12-3.1.jar \
  /path/to/your-spark-app.jar
```

### 4.3 两种方式怎么选

建议直接按下面选：

- 第一次打通链路：用 `spark-shell`
- 已经有正式作业代码：用程序调用 + `spark-submit`
- 排查网络和参数问题：优先用 `spark-shell`
- 上线跑批任务：用 `spark-submit`

## 5. 开发示例

本节是本文重点。下面的示例都默认你已经把 connector `jar` 带进 Spark 了。

### 5.1 示例一：从 Spark 表写入 YMatrix

这是最常见的正式开发方式，适合源数据已经是 Spark 表或临时视图的场景。

```scala
import org.apache.spark.sql.functions.col

val sourceTable = "dm.orders_daily"
val targetTable = "orders_daily"

val sourceDf = spark.table(sourceTable)
  .select(
    col("order_id"),
    col("user_id"),
    col("shop_id"),
    col("pay_amount").cast("decimal(18,2)").as("pay_amount"),
    col("status"),
    col("created_at").cast("timestamp").as("created_at")
  )
  .repartition(4, col("order_id"))

sourceDf.write
  .format("its-ymatrix")
  .option("url", sys.env("YMATRIX_URL"))
  .option("user", sys.env("YMATRIX_USER"))
  .option("password", sys.env("YMATRIX_PASSWORD"))
  .option("dbschema", sys.env.getOrElse("YMATRIX_SCHEMA", "public"))
  .option("dbtable", targetTable)
  .option("distributedby", "order_id")
  .option("network.timeout", "120s")
  .option("server.timeout", "120s")
  .mode("append")
  .save()
```

适合什么时候用：

- 你已经有标准 Spark SQL 表
- 你希望把字段清单显式写清楚
- 你希望在写入前控制分区数

### 5.2 示例二：写入前做过滤和字段映射

如果目标表字段与源表字段不完全一致，推荐在 Spark 内先做映射，再写入。

```scala
import org.apache.spark.sql.functions._

val mappedDf = spark.table("ods.orders_raw")
  .where(col("dt") === lit("2026-03-23"))
  .where(col("is_deleted") === lit(0))
  .select(
    col("id").as("biz_order_id"),
    col("buyer").as("buyer_id"),
    col("seller").as("seller_id"),
    col("amount").cast("decimal(18,2)").as("pay_amount"),
    to_timestamp(col("pay_time")).as("pay_time")
  )

mappedDf.write
  .format("its-ymatrix")
  .option("url", sys.env("YMATRIX_URL"))
  .option("user", sys.env("YMATRIX_USER"))
  .option("password", sys.env("YMATRIX_PASSWORD"))
  .option("dbschema", sys.env.getOrElse("YMATRIX_SCHEMA", "public"))
  .option("dbtable", "orders_mapped")
  .option("distributedby", "biz_order_id")
  .mode("overwrite")
  .save()
```

适合什么时候用：

- 需要过滤脏数据
- 需要重命名字段
- 需要显式 `cast` 金额、时间等关键类型

### 5.3 示例三：覆盖写入但保留目标表结构

开发环境经常需要“清空旧数据重新导入”，但不想重建表。

```scala
spark.table("dm.orders_demo")
  .write
  .format("its-ymatrix")
  .option("url", sys.env("YMATRIX_URL"))
  .option("user", sys.env("YMATRIX_USER"))
  .option("password", sys.env("YMATRIX_PASSWORD"))
  .option("dbschema", sys.env.getOrElse("YMATRIX_SCHEMA", "public"))
  .option("dbtable", "orders_demo")
  .option("truncate", "true")
  .mode("overwrite")
  .save()
```

适合什么时候用：

- 开发联调
- 测试环境反复重跑
- 想保留表结构但重导数据

### 5.4 示例四：按范围分批导入大表

大表第一次导入时，不建议直接整表全量压上去。更稳妥的方式是基于单调字段按区间推进。

```scala
import org.apache.spark.sql.functions._

val sourceTable = "dwd.iot_orders"
val lowerBound = 1L
val upperBound = 100000L

val batchDf = spark.table(sourceTable)
  .where(col("ingest_id").between(lowerBound, upperBound))
  .select(
    col("ingest_id"),
    col("event_id"),
    col("device_id"),
    col("tenant_id"),
    col("event_time").cast("timestamp"),
    col("payload")
  )
  .repartition(4, col("ingest_id"))

batchDf.write
  .format("its-ymatrix")
  .option("url", sys.env("YMATRIX_URL"))
  .option("user", sys.env("YMATRIX_USER"))
  .option("password", sys.env("YMATRIX_PASSWORD"))
  .option("dbschema", sys.env.getOrElse("YMATRIX_SCHEMA", "public"))
  .option("dbtable", "iot_orders_batch_demo")
  .option("distributedby", "ingest_id")
  .option("server.port", "43001")
  .mode("append")
  .save()
```

适合什么时候用：

- 大表首次迁移
- 灰度导入
- 分批补数
- 人工控制每批数据量

### 5.5 示例五：使用 Spark SQL 写入

如果你的主流程偏 SQL 风格，而不是 DataFrame API，可以使用临时 sink view。

先在 YMatrix 中创建目标表：

```sql
create table public.orders_sql_sink (
  order_id bigint,
  user_id text,
  amount decimal(18,2),
  created_at timestamp
)
distributed by (order_id);
```

然后在 Spark 中执行：

```scala
import spark.implicits._
import org.apache.spark.sql.functions._

Seq(
  (1L, "u001", BigDecimal("18.50"), "2026-03-23 10:00:00"),
  (2L, "u002", BigDecimal("20.00"), "2026-03-23 10:05:00")
).toDF("order_id", "user_id", "amount", "created_at")
  .withColumn("created_at", col("created_at").cast("timestamp"))
  .createOrReplaceTempView("spark_orders_src")

spark.sql("DROP VIEW IF EXISTS ymatrix_orders_sink")

spark.sql(
  s"""
     |CREATE TEMPORARY VIEW ymatrix_orders_sink
     |USING com.itsumma.gpconnector.GreenplumDataSource
     |OPTIONS (
     |  url '${sys.env("YMATRIX_URL")}',
     |  user '${sys.env("YMATRIX_USER")}',
     |  password '${sys.env("YMATRIX_PASSWORD")}',
     |  dbschema '${sys.env.getOrElse("YMATRIX_SCHEMA", "public")}',
     |  dbtable 'orders_sql_sink',
     |  network.timeout '120s',
     |  server.timeout '120s',
     |  dbmessages 'WARN'
     |)
     |""".stripMargin
)

spark.sql(
  """
    |INSERT INTO TABLE ymatrix_orders_sink
    |SELECT /*+ REPARTITION(4, order_id) */
    |  order_id,
    |  user_id,
    |  amount,
    |  created_at
    |FROM spark_orders_src
    |""".stripMargin
)
```

这个方式的重点：

- 更适合 SQL 风格作业
- 目标表需要预先存在
- `INSERT` 侧建议显式列出字段

### 5.6 示例六：写后校验

写入完成后，建议至少做一次基础校验。

行数校验：

```scala
spark.read
  .format("its-ymatrix")
  .option("url", sys.env("YMATRIX_URL"))
  .option("user", sys.env("YMATRIX_USER"))
  .option("password", sys.env("YMATRIX_PASSWORD"))
  .option(
    "dbtable",
    s"select count(*)::bigint as cnt from ${sys.env.getOrElse("YMATRIX_SCHEMA", "public")}.orders_demo"
  )
  .load()
  .show(false)
```

主键或水位范围校验：

```scala
spark.read
  .format("its-ymatrix")
  .option("url", sys.env("YMATRIX_URL"))
  .option("user", sys.env("YMATRIX_USER"))
  .option("password", sys.env("YMATRIX_PASSWORD"))
  .option(
    "dbtable",
    s"select min(order_id) as min_id, max(order_id) as max_id from ${sys.env.getOrElse("YMATRIX_SCHEMA", "public")}.orders_demo"
  )
  .load()
  .show(false)
```

推荐至少做三类校验：

1. 行数校验
2. 主键或水位范围校验
3. 抽样数据校验

## 6. 常见问题与注意事项

### 6.1 写入时必须显式指定模式

写入时必须明确使用：

- `mode("append")`
- `mode("overwrite")`

不要省略写入模式。

### 6.2 目标表是否会自动创建

如果目标表不存在，connector 可以在部分场景下自动建表；但正式生产环境仍建议对核心表采用预建表方式，显式管理：

- 列类型
- 分布策略
- 存储属性

### 6.3 排查网络问题时看什么

如果报错与 `gpfdist` 连接有关，优先排查：

- `SPARK_LOCAL_IP` 是否为 YMatrix segment 可访问地址
- Spark Driver / Executor 所在主机端口是否放通
- 是否需要固定 `server.port` 便于联调

### 6.4 分区数怎么理解

`repartition` 主要影响写入并发。

经验上建议：

- 小表联调先用较小分区数
- 大表导入时让分布键参与 `repartition`
- 分区数不要明显大于 YMatrix primary segment 数量

### 6.5 哪些类型要重点验证

正式导入前，建议重点验证以下字段类型：

- `decimal`
- `timestamp`
- `date`
- JSON / 几何等扩展类型
- 超宽字符串字段

### 6.6 推荐上手顺序

建议按下面顺序推进：

1. 先用 `spark-shell` 跑最小 3 行样例
2. 再用正式源表做单表导入
3. 再验证字段映射和类型兼容
4. 最后做大表分批导入或批量导入

这样最容易把 jar、参数、网络、结构和性能问题拆开定位。
