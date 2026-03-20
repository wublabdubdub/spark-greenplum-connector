# Iceberg 到 Greenplum GPFDIST 迁移手册

## 1. 目标

本文档说明如何通过当前仓库内置的 `its-ymatrix` connector，使用 `gpfdist` 路径把 Iceberg 表：

- `local.test_db.iot_wide_0001`

迁移到 Greenplum 表：

- `public.iot_wide_0001`

本文覆盖：

1. `psql` / Greenplum SQL Shell 建表
2. `spark-shell` / Scala 交互式迁移
3. Scala 主程序迁移
4. `gpfdist` 路径的 SQL 能力边界说明

## 2. 先说结论

`gpfdist` 路径和 JDBC 路径是两套不同方案。

当前仓库里的 `gpfdist` 路径本质上是：

```text
Iceberg table
  -> Spark DataFrame
  -> format("its-ymatrix")
  -> GPFDIST + JDBC metadata
  -> Greenplum
```

也就是说：

- 目标表可以先在 Greenplum 里 `CREATE TABLE`
- 迁移动作本身由 Spark DataFrameWriter 触发
- 当前仓库的标准入口是 `.format("its-ymatrix")`
- 不是 Spark 内置 JDBC catalog
- 不是 Flink 风格的 `WITH ('connector'='...')`

## 3. 与 JDBC 路径的差异

这条路径与 JDBC 手册中的做法有三个核心区别：

1. 这里必须使用 connector JAR
2. 这里底层走的是 `gpfdist`
3. Greenplum segment 必须能够反向访问 Spark 节点

所以你可以把两条路径简单理解成：

### 3.1 JDBC 路径

```text
Spark 主动连 Greenplum
```

### 3.2 GPFDIST 路径

```text
Spark 与 Greenplum segment 双向通信
Greenplum segment 需要回连 Spark 暴露的 gpfdist 服务
```

## 4. 前置条件

在当前环境中，`gpfdist` 路径只有在网络满足时才可用。

必须同时满足：

1. Spark 能连接 Greenplum 主库
2. Greenplum 各 segment 也能反向访问 Spark Worker 节点

如果你看到类似错误：

```text
ERROR: error when connecting to gpfdist http://172.16.100.143:33253/output.pipe, quit after 11 tries
(seg13 slice1 192.168.100.30:6005 pid=168824)
```

那就说明：

- Spark 已经拉起了一个临时 `gpfdist` 服务
- 但 `192.168.100.x` 网段上的 Greenplum segment 连不回 Spark 机器
- 这时作业会表现成“卡住”，本质上是在等网络超时

## 5. 当前环境参数

本文中的固定参数如下：

```text
Iceberg catalog:    local
Iceberg warehouse:  /data/iceberg/warehouse
Iceberg source:     local.test_db.iot_wide_0001

Greenplum URL:      jdbc:postgresql://172.16.100.29:5432/zhangchen
Greenplum user:     zhangchen
Greenplum password: YMatrix@123
Greenplum schema:   public
Greenplum table:    iot_wide_0001

分布键:              ingest_id
connector shortName: its-ymatrix
```

## 6. 目标表 DDL

无论走 `gpfdist` 还是 JDBC，目标表 DDL 都可以统一先在 Greenplum 侧建好。

```sql
DROP TABLE IF EXISTS public.iot_wide_0001;

CREATE TABLE public.iot_wide_0001 (
  ingest_id bigint,
  event_id text,
  device_id text,
  tenant_id integer,
  site_id integer,
  line_id text,
  region_code text,
  event_time timestamp,
  event_date date,
  event_minute text,
  status_code integer,
  alarm_level integer,
  metric_01 double precision,
  metric_02 double precision,
  metric_03 double precision,
  metric_04 double precision,
  metric_05 double precision,
  metric_06 double precision,
  metric_07 double precision,
  metric_08 double precision,
  metric_09 double precision,
  metric_10 double precision,
  metric_11 double precision,
  metric_12 double precision,
  metric_13 double precision,
  metric_14 double precision,
  metric_15 double precision,
  metric_16 double precision,
  metric_17 double precision,
  metric_18 double precision,
  metric_19 double precision,
  metric_20 double precision,
  counter_01 bigint,
  counter_02 bigint,
  counter_03 bigint,
  counter_04 bigint,
  counter_05 bigint,
  attr_01 text,
  attr_02 text,
  attr_03 text,
  attr_04 text,
  attr_05 text,
  tag_01 text,
  tag_02 text,
  tag_03 text,
  ext_json text
)
DISTRIBUTED BY (ingest_id);
```

## 7. 方式一：Greenplum SQL Shell 建表 + spark-shell / Scala 迁移

这是当前代码库里最标准、最可控的 `gpfdist` 使用方式。

### 7.1 第一步：在 Greenplum 侧建表

在 `psql` 或任意 SQL 客户端里执行上一节 DDL。

### 7.2 第二步：确认 connector JAR

```bash
ls -lh /root/spark-greenplum-connector/spark-greenplum-connector/target/spark-ymatrix-connector_2.12-3.1.jar
```

如果这个 JAR 不存在，先编译或准备好对应版本。

### 7.3 第三步：启动 spark-shell

```bash
export SPARK_LOCAL_IP=172.16.100.143

spark-shell \
  --master local[8] \
  --driver-memory 16g \
  --conf spark.driver.host=${SPARK_LOCAL_IP} \
  --conf spark.driver.bindAddress=0.0.0.0 \
  --conf spark.local.ip=${SPARK_LOCAL_IP} \
  --conf spark.sql.shuffle.partitions=8 \
  --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.local.type=hadoop \
  --conf spark.sql.catalog.local.warehouse=/data/iceberg/warehouse \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --jars /root/spark-greenplum-connector/spark-greenplum-connector/target/spark-ymatrix-connector_2.12-3.1.jar
```

说明：

- `SPARK_LOCAL_IP` 很重要
- 它必须设置成 Greenplum segment 可达的地址
- 如果这个地址对 segment 不可达，`gpfdist` 会失败

### 7.4 第四步：在 Scala 里执行迁移

```scala
import org.apache.spark.sql.functions.col

val sourceDf = spark.table("local.test_db.iot_wide_0001")

sourceDf
  .repartition(8, col("ingest_id"))
  .write
  .format("its-ymatrix")
  .option("url", "jdbc:postgresql://172.16.100.29:5432/zhangchen")
  .option("user", "zhangchen")
  .option("password", "YMatrix@123")
  .option("dbschema", "public")
  .option("dbtable", "iot_wide_0001")
  .option("distributedby", "ingest_id")
  .option("network.timeout", "300s")
  .option("server.timeout", "300s")
  .option("dbmessages", "WARN")
  .mode("append")
  .save()
```

如果你希望由 connector 自动重建目标表，可以改成：

```scala
.mode("overwrite")
```

### 7.5 第五步：校验结果

在 Spark 中：

```scala
val verifyDf = spark.read
  .format("its-ymatrix")
  .option("url", "jdbc:postgresql://172.16.100.29:5432/zhangchen")
  .option("user", "zhangchen")
  .option("password", "YMatrix@123")
  .option("dbtable", "select count(*)::bigint as cnt, coalesce(min(ingest_id),0)::bigint as min_id, coalesce(max(ingest_id),0)::bigint as max_id from public.iot_wide_0001")
  .load()

verifyDf.show(false)
```

或者在 Greenplum 里：

```sql
SELECT count(*) FROM public.iot_wide_0001;
SELECT min(ingest_id), max(ingest_id) FROM public.iot_wide_0001;
```

## 8. 方式二：使用仓库内置脚本

当前仓库已经有一套可直接执行的 `gpfdist` 脚本：

- [migrate-iceberg-iot-wide-0001-full-load.scala](/root/spark-greenplum-connector/scripts/migrate-iceberg-iot-wide-0001-full-load.scala)
- [run-iceberg-iot-wide-0001-full-load.sh](/root/spark-greenplum-connector/scripts/run-iceberg-iot-wide-0001-full-load.sh)

执行方式：

```bash
chmod +x /root/spark-greenplum-connector/scripts/run-iceberg-iot-wide-0001-full-load.sh
/root/spark-greenplum-connector/scripts/run-iceberg-iot-wide-0001-full-load.sh
```

这套脚本的逻辑是：

1. 读取 `local.test_db.iot_wide_0001`
2. 校验源表 `count(*)` 和 `ingest_id` 范围
3. 通过 `.format("its-ymatrix")` 写入 `public.iot_wide_0001`
4. 再回查 Greenplum 做结果校验

## 9. 方式三：Scala 主程序

如果你要做成一个正式 Spark 程序，可以用下面这个模板。

```scala
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object IcebergToGreenplumGpfdistApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("iceberg-to-greenplum-gpfdist")
      .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.local.type", "hadoop")
      .config("spark.sql.catalog.local.warehouse", "/data/iceberg/warehouse")
      .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .getOrCreate()

    spark.table("local.test_db.iot_wide_0001")
      .repartition(8, col("ingest_id"))
      .write
      .format("its-ymatrix")
      .option("url", "jdbc:postgresql://172.16.100.29:5432/zhangchen")
      .option("user", "zhangchen")
      .option("password", "YMatrix@123")
      .option("dbschema", "public")
      .option("dbtable", "iot_wide_0001")
      .option("distributedby", "ingest_id")
      .option("network.timeout", "300s")
      .option("server.timeout", "300s")
      .option("dbmessages", "WARN")
      .mode("overwrite")
      .save()

    spark.stop()
  }
}
```

提交时要带 connector JAR：

```bash
spark-submit \
  --jars /root/spark-greenplum-connector/spark-greenplum-connector/target/spark-ymatrix-connector_2.12-3.1.jar \
  your-app.jar
```

## 10. 关于“CREATE TABLE + INSERT”的说明

这里需要明确区分 Greenplum SQL 和 Spark SQL。

### 10.1 Greenplum 侧可以 `CREATE TABLE`

这部分完全没有问题，也推荐先手工建好目标表：

```sql
CREATE TABLE public.iot_wide_0001 ( ... ) DISTRIBUTED BY (ingest_id);
```

### 10.2 Spark 侧的迁移动作，不建议强行写成 JDBC catalog 那种 `INSERT INTO gp.public.xxx`

因为那条路已经变成 JDBC 方案了，不再是 `gpfdist`。

### 10.3 当前 connector 的标准写法不是 Flink 那种

也就是说，当前仓库不提供这样的标准入口：

```sql
CREATE TABLE sink_xxx (...) WITH (...)
INSERT INTO sink_xxx SELECT ...
```

当前仓库真正稳定、可用的入口是：

```scala
df.write.format("its-ymatrix").options(...).save()
```

## 11. 如果你坚持要写成“纯 SQL”

这件事在当前代码库里要谨慎看待。

理论上最接近的写法会是：

```sql
CREATE TEMPORARY VIEW gp_sink
USING its-ymatrix
OPTIONS (
  url 'jdbc:postgresql://172.16.100.29:5432/zhangchen',
  user 'zhangchen',
  password 'YMatrix@123',
  dbschema 'public',
  dbtable 'iot_wide_0001',
  distributedby 'ingest_id',
  network.timeout '300s',
  server.timeout '300s'
);

INSERT INTO TABLE gp_sink
SELECT *
FROM local.test_db.iot_wide_0001;
```

但是要注意：

- 这不是当前仓库的主路径
- 当前 connector 的 SQL DDL 能力没有像 JDBC catalog 那样成熟
- 某些 Spark 版本和 connector 组合下，可能会在 schema 推断或写入阶段遇到限制
- 因此这条路更适合作为探索，不适合作为当前主文档里的标准执行方案

## 12. 常见问题

### 12.1 为什么 `gpfdist` 路径必须带 connector JAR

因为 `its-ymatrix` 不是 Spark 内置 datasource。

这个 JAR 里包含了：

- `its-ymatrix` 数据源注册
- `gpfdist` 传输逻辑
- Spark 与 Greenplum 之间的元数据和写入协调逻辑

没有这个 JAR，Spark 根本不知道 `its-ymatrix` 是什么。

### 12.2 为什么 JDBC 路径不需要这个 connector JAR

因为 JDBC 路径使用的是：

- Spark 内置 JDBC datasource
- Spark JDBC catalog
- PostgreSQL JDBC Driver

这时 Spark 并没有调用 `its-ymatrix`。

### 12.3 什么时候优先用 `gpfdist`

在下面场景可以优先考虑：

- 网络已经打通
- 需要更高吞吐
- 希望使用当前仓库原生 connector 的并行写入能力

### 12.4 什么时候优先用 JDBC

在下面场景优先用 JDBC：

- 当前网络无法让 Greenplum segment 回连 Spark
- 需要最简单稳定的迁移方式
- 更看重可执行性，而不是峰值吞吐

## 13. 最短模板

如果只保留最核心的 `gpfdist` 迁移代码，就是下面这段：

```scala
spark.table("local.test_db.iot_wide_0001")
  .repartition(8, col("ingest_id"))
  .write
  .format("its-ymatrix")
  .option("url", "jdbc:postgresql://172.16.100.29:5432/zhangchen")
  .option("user", "zhangchen")
  .option("password", "YMatrix@123")
  .option("dbschema", "public")
  .option("dbtable", "iot_wide_0001")
  .option("distributedby", "ingest_id")
  .option("network.timeout", "300s")
  .option("server.timeout", "300s")
  .mode("overwrite")
  .save()
```

如果你需要的是“当前环境最容易成功”，优先走 JDBC 手册。  
如果你需要的是“网络打通后的高吞吐 connector 路径”，就按本文走 `gpfdist`。
