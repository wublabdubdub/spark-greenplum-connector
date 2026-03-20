# Iceberg 到 Greenplum JDBC SQL 迁移手册

## 1. 目标

本文档说明如何在当前环境下，通过 JDBC 路径把 Iceberg 表：

- `local.test_db.iot_wide_0001`

迁移到 Greenplum 表：

- `gp.public.iot_wide_0001`

本文覆盖以下几种方式：

1. `psql` / Greenplum SQL Shell 建表 + `spark-sql` 执行 `INSERT`
2. `spark-shell` / Scala 交互式执行
3. Scala 主程序方式
4. PySpark 程序方式

本文统一采用 `CREATE TABLE` + `INSERT INTO ... SELECT ...` 的思路。

## 2. 先说结论

在你当前环境里，推荐的 JDBC SQL 迁移模式是：

```text
Greenplum 侧先 CREATE TABLE
Spark 侧通过 JDBC catalog 暴露 gp.public.iot_wide_0001
然后执行 INSERT INTO gp.public.iot_wide_0001
SELECT * FROM local.test_db.iot_wide_0001
```

也就是：

- 目标表通过 Greenplum SQL 或普通 JDBC 先建好
- Spark 侧不使用 `gpfdist`
- Spark 侧不使用 `CREATE TABLE ... USING jdbc`
- Spark 侧通过 `JDBCTableCatalog` 把 Greenplum 映射成一个 catalog table

## 3. 当前环境参数

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
```

## 4. 为什么推荐 JDBC catalog

推荐原因很简单：

- JDBC 路径只要求 Spark 主动连接 Greenplum
- 不要求 Greenplum segment 反向回连 Spark
- 因此不会触发 `gpfdist` 的回连失败问题

与之对应，这几种写法在你这套环境里不要作为主路径：

```sql
CREATE TABLE xxx USING jdbc ...
CREATE TABLE xxx USING org.apache.spark.sql.jdbc ...
```

你前面已经实际验证过，这两种会报：

```text
Unsupported format in USING: jdbc
Unsupported format in USING: org.apache.spark.sql.jdbc
```

所以在当前环境中，正确思路不是在 `spark_catalog` 下创建 JDBC sink table，而是：

```sql
INSERT INTO gp.public.iot_wide_0001
SELECT *
FROM local.test_db.iot_wide_0001;
```

## 5. 目标表 DDL

下面给出 `iot_wide_0001` 可直接使用的 Greenplum 建表语句。

这个 schema 来自当前仓库里 Iceberg 宽表造数脚本，字段顺序已经和源表保持一致。

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

如果你不想删表，也可以改成：

```sql
TRUNCATE TABLE public.iot_wide_0001;
```

前提是表已经存在且字段结构完全一致。

## 6. 方式一：Greenplum SQL Shell 建表 + spark-sql 执行 INSERT

这是最推荐的方式。

### 6.1 第一步：在 Greenplum 侧建表

在 `psql` 或任意能连接 Greenplum 的 SQL 客户端里执行上一节的 DDL。

### 6.2 第二步：启动 spark-sql

```bash
spark-sql \
  --packages org.postgresql:postgresql:42.7.2 \
  --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.local.type=hadoop \
  --conf spark.sql.catalog.local.warehouse=/data/iceberg/warehouse \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.gp=org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCTableCatalog \
  --conf spark.sql.catalog.gp.url=jdbc:postgresql://172.16.100.29:5432/zhangchen \
  --conf spark.sql.catalog.gp.user=zhangchen \
  --conf spark.sql.catalog.gp.password=YMatrix@123 \
  --conf spark.sql.catalog.gp.driver=org.postgresql.Driver
```

### 6.3 第三步：执行迁移 SQL

先做简单检查：

```sql
SHOW TABLES IN gp.public;
SELECT count(*) FROM local.test_db.iot_wide_0001;
SELECT min(ingest_id), max(ingest_id) FROM local.test_db.iot_wide_0001;
```

再执行迁移：

```sql
INSERT INTO `gp`.`public`.`iot_wide_0001`
SELECT *
FROM `local`.`test_db`.`iot_wide_0001`;
```

### 6.4 第四步：校验结果

```sql
SELECT count(*) FROM `gp`.`public`.`iot_wide_0001`;
SELECT min(ingest_id), max(ingest_id) FROM `gp`.`public`.`iot_wide_0001`;
```

### 6.5 一条命令直接执行

如果你不想进入交互式 `spark-sql`，可以直接：

```bash
spark-sql \
  --packages org.postgresql:postgresql:42.7.2 \
  --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.local.type=hadoop \
  --conf spark.sql.catalog.local.warehouse=/data/iceberg/warehouse \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.gp=org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCTableCatalog \
  --conf spark.sql.catalog.gp.url=jdbc:postgresql://172.16.100.29:5432/zhangchen \
  --conf spark.sql.catalog.gp.user=zhangchen \
  --conf spark.sql.catalog.gp.password=YMatrix@123 \
  --conf spark.sql.catalog.gp.driver=org.postgresql.Driver \
  -e "
    INSERT INTO \`gp\`.\`public\`.\`iot_wide_0001\`
    SELECT *
    FROM \`local\`.\`test_db\`.\`iot_wide_0001\`
  "
```

## 7. 方式二：spark-shell / Scala 交互式执行

### 7.1 启动 spark-shell

```bash
spark-shell \
  --packages org.postgresql:postgresql:42.7.2 \
  --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.local.type=hadoop \
  --conf spark.sql.catalog.local.warehouse=/data/iceberg/warehouse \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.gp=org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCTableCatalog \
  --conf spark.sql.catalog.gp.url=jdbc:postgresql://172.16.100.29:5432/zhangchen \
  --conf spark.sql.catalog.gp.user=zhangchen \
  --conf spark.sql.catalog.gp.password=YMatrix@123 \
  --conf spark.sql.catalog.gp.driver=org.postgresql.Driver
```

### 7.2 在 Scala 中建表并迁移

这段代码的思路是：

1. 通过普通 JDBC 执行 `CREATE TABLE`
2. 再通过 `spark.sql` 执行 `INSERT INTO ... SELECT ...`

```scala
import java.sql.DriverManager

val ddl =
  """
    |DROP TABLE IF EXISTS public.iot_wide_0001;
    |CREATE TABLE public.iot_wide_0001 (
    |  ingest_id bigint,
    |  event_id text,
    |  device_id text,
    |  tenant_id integer,
    |  site_id integer,
    |  line_id text,
    |  region_code text,
    |  event_time timestamp,
    |  event_date date,
    |  event_minute text,
    |  status_code integer,
    |  alarm_level integer,
    |  metric_01 double precision,
    |  metric_02 double precision,
    |  metric_03 double precision,
    |  metric_04 double precision,
    |  metric_05 double precision,
    |  metric_06 double precision,
    |  metric_07 double precision,
    |  metric_08 double precision,
    |  metric_09 double precision,
    |  metric_10 double precision,
    |  metric_11 double precision,
    |  metric_12 double precision,
    |  metric_13 double precision,
    |  metric_14 double precision,
    |  metric_15 double precision,
    |  metric_16 double precision,
    |  metric_17 double precision,
    |  metric_18 double precision,
    |  metric_19 double precision,
    |  metric_20 double precision,
    |  counter_01 bigint,
    |  counter_02 bigint,
    |  counter_03 bigint,
    |  counter_04 bigint,
    |  counter_05 bigint,
    |  attr_01 text,
    |  attr_02 text,
    |  attr_03 text,
    |  attr_04 text,
    |  attr_05 text,
    |  tag_01 text,
    |  tag_02 text,
    |  tag_03 text,
    |  ext_json text
    |)
    |DISTRIBUTED BY (ingest_id)
    |""".stripMargin

val conn = DriverManager.getConnection(
  "jdbc:postgresql://172.16.100.29:5432/zhangchen",
  "zhangchen",
  "YMatrix@123"
)

try {
  val stmt = conn.createStatement()
  try {
    ddl.split(";").map(_.trim).filter(_.nonEmpty).foreach(stmt.execute)
  } finally {
    stmt.close()
  }
} finally {
  conn.close()
}

spark.sql("""
  INSERT INTO `gp`.`public`.`iot_wide_0001`
  SELECT *
  FROM `local`.`test_db`.`iot_wide_0001`
""")

spark.sql("SELECT count(*) FROM `gp`.`public`.`iot_wide_0001`").show(false)
spark.sql("SELECT min(ingest_id), max(ingest_id) FROM `gp`.`public`.`iot_wide_0001`").show(false)
```

## 8. 方式三：Scala 主程序

下面给出一个可直接放进工程里的 Scala 主程序示例。

```scala
import java.sql.DriverManager
import org.apache.spark.sql.SparkSession

object IcebergToGreenplumJdbcSqlApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("iceberg-to-greenplum-jdbc-sql")
      .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.local.type", "hadoop")
      .config("spark.sql.catalog.local.warehouse", "/data/iceberg/warehouse")
      .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .config("spark.sql.catalog.gp", "org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCTableCatalog")
      .config("spark.sql.catalog.gp.url", "jdbc:postgresql://172.16.100.29:5432/zhangchen")
      .config("spark.sql.catalog.gp.user", "zhangchen")
      .config("spark.sql.catalog.gp.password", "YMatrix@123")
      .config("spark.sql.catalog.gp.driver", "org.postgresql.Driver")
      .getOrCreate()

    val ddl =
      """
        |DROP TABLE IF EXISTS public.iot_wide_0001;
        |CREATE TABLE public.iot_wide_0001 (
        |  ingest_id bigint,
        |  event_id text,
        |  device_id text,
        |  tenant_id integer,
        |  site_id integer,
        |  line_id text,
        |  region_code text,
        |  event_time timestamp,
        |  event_date date,
        |  event_minute text,
        |  status_code integer,
        |  alarm_level integer,
        |  metric_01 double precision,
        |  metric_02 double precision,
        |  metric_03 double precision,
        |  metric_04 double precision,
        |  metric_05 double precision,
        |  metric_06 double precision,
        |  metric_07 double precision,
        |  metric_08 double precision,
        |  metric_09 double precision,
        |  metric_10 double precision,
        |  metric_11 double precision,
        |  metric_12 double precision,
        |  metric_13 double precision,
        |  metric_14 double precision,
        |  metric_15 double precision,
        |  metric_16 double precision,
        |  metric_17 double precision,
        |  metric_18 double precision,
        |  metric_19 double precision,
        |  metric_20 double precision,
        |  counter_01 bigint,
        |  counter_02 bigint,
        |  counter_03 bigint,
        |  counter_04 bigint,
        |  counter_05 bigint,
        |  attr_01 text,
        |  attr_02 text,
        |  attr_03 text,
        |  attr_04 text,
        |  attr_05 text,
        |  tag_01 text,
        |  tag_02 text,
        |  tag_03 text,
        |  ext_json text
        |)
        |DISTRIBUTED BY (ingest_id)
        |""".stripMargin

    val conn = DriverManager.getConnection(
      "jdbc:postgresql://172.16.100.29:5432/zhangchen",
      "zhangchen",
      "YMatrix@123"
    )

    try {
      val stmt = conn.createStatement()
      try {
        ddl.split(";").map(_.trim).filter(_.nonEmpty).foreach(stmt.execute)
      } finally {
        stmt.close()
      }
    } finally {
      conn.close()
    }

    spark.sql("""
      INSERT INTO `gp`.`public`.`iot_wide_0001`
      SELECT *
      FROM `local`.`test_db`.`iot_wide_0001`
    """)

    spark.stop()
  }
}
```

提交程序时：

```bash
spark-submit \
  --packages org.postgresql:postgresql:42.7.2 \
  your-app.jar
```

如果机器不能联网拉包，就先下载 PostgreSQL JDBC Driver，再改用：

```bash
spark-submit \
  --jars /path/to/postgresql-42.7.2.jar \
  your-app.jar
```

## 9. 方式四：PySpark 程序

如果你更喜欢 Python，可以这样写。

```python
from pyspark.sql import SparkSession
import psycopg2

spark = (
    SparkSession.builder
    .appName("iceberg-to-greenplum-jdbc-sql")
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.local.type", "hadoop")
    .config("spark.sql.catalog.local.warehouse", "/data/iceberg/warehouse")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.gp", "org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCTableCatalog")
    .config("spark.sql.catalog.gp.url", "jdbc:postgresql://172.16.100.29:5432/zhangchen")
    .config("spark.sql.catalog.gp.user", "zhangchen")
    .config("spark.sql.catalog.gp.password", "YMatrix@123")
    .config("spark.sql.catalog.gp.driver", "org.postgresql.Driver")
    .getOrCreate()
)

ddl = """
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
DISTRIBUTED BY (ingest_id)
"""

conn = psycopg2.connect(
    host="172.16.100.29",
    port=5432,
    dbname="zhangchen",
    user="zhangchen",
    password="YMatrix@123",
)
conn.autocommit = True
cur = conn.cursor()
for stmt in [s.strip() for s in ddl.split(";") if s.strip()]:
    cur.execute(stmt)
cur.close()
conn.close()

spark.sql("""
INSERT INTO `gp`.`public`.`iot_wide_0001`
SELECT *
FROM `local`.`test_db`.`iot_wide_0001`
""")

spark.stop()
```

## 10. 常见问题

### 10.1 为什么不直接用 gpfdist

因为 JDBC 路径不需要 Greenplum segment 反向访问 Spark 节点，所以更适合你当前的网络环境。

### 10.2 为什么不直接用 `CREATE TABLE ... USING jdbc`

因为你当前环境里已经实际报过：

```text
Unsupported format in USING: jdbc
Unsupported format in USING: org.apache.spark.sql.jdbc
```

所以这条路在当前环境下不要继续尝试。

### 10.3 为什么 `INSERT INTO gp.public.iot_wide_0001 SELECT * ...` 可以

因为这里的 `gp` 不是普通表名，而是 Spark 里的 JDBC catalog：

- `local.test_db.iot_wide_0001` 是 Iceberg catalog 表
- `gp.public.iot_wide_0001` 是 JDBC catalog 表

Spark SQL 会负责把源表读出来，再通过 JDBC 写入目标表。

### 10.4 如果字段顺序不完全一致怎么办

不要用 `SELECT *`，改成显式字段列表：

```sql
INSERT INTO `gp`.`public`.`iot_wide_0001`
SELECT
  ingest_id,
  event_id,
  device_id,
  tenant_id,
  site_id,
  line_id,
  region_code,
  event_time,
  event_date,
  event_minute,
  status_code,
  alarm_level,
  metric_01,
  metric_02,
  metric_03,
  metric_04,
  metric_05,
  metric_06,
  metric_07,
  metric_08,
  metric_09,
  metric_10,
  metric_11,
  metric_12,
  metric_13,
  metric_14,
  metric_15,
  metric_16,
  metric_17,
  metric_18,
  metric_19,
  metric_20,
  counter_01,
  counter_02,
  counter_03,
  counter_04,
  counter_05,
  attr_01,
  attr_02,
  attr_03,
  attr_04,
  attr_05,
  tag_01,
  tag_02,
  tag_03,
  ext_json
FROM `local`.`test_db`.`iot_wide_0001`;
```

## 11. 最短可执行模板

如果只保留最核心步骤，就是下面两步。

第一步，在 Greenplum 里：

```sql
DROP TABLE IF EXISTS public.iot_wide_0001;
CREATE TABLE public.iot_wide_0001 ( ... ) DISTRIBUTED BY (ingest_id);
```

第二步，在 Spark 里：

```sql
INSERT INTO `gp`.`public`.`iot_wide_0001`
SELECT *
FROM `local`.`test_db`.`iot_wide_0001`;
```

这就是当前环境下最稳定、最清晰的 JDBC SQL 迁移路径。
