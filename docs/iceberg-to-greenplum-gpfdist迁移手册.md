# Iceberg 到 Greenplum GPFDIST 迁移手册

## 1. 目标

本文档只讲一件事：如何通过当前仓库内置的 `its-ymatrix` connector，走 `gpfdist` 路径，把 Iceberg 表 `local.test_db.iot_wide_0001` 同步到 Greenplum 表 `public.iot_wide_0001`。

本文提供 3 种可直接照抄执行的方式：

1. `spark-shell` 交互式 Scala 写法
2. `spark-shell` 中使用 `CREATE TEMPORARY VIEW ... USING ...` 的 SQL 写法
3. 使用仓库现成脚本直接执行

这 3 种方式都覆盖：

- 登录机器
- 进入项目目录
- 准备目标表
- 启动 Spark
- 执行迁移
- 校验结果

## 2. 先说结论

当前仓库里的 GPFDIST 迁移链路本质如下：

```text
Iceberg
  -> Spark DataFrame / Spark SQL
  -> its-ymatrix connector
  -> GPFDIST + JDBC metadata
  -> Greenplum
```

关键点：

- 真正的数据传输走的是 `gpfdist`
- `url / user / password` 这些参数仍然必需，因为 connector 还要通过 JDBC 做元数据交互
- Greenplum segment 必须能回连 Spark 暴露出来的 `gpfdist` 地址
- 如果网络不通，常见错误不是 SQL 语法错，而是 `error when connecting to gpfdist`

## 3. 本文固定参数

本文下面所有命令默认使用以下参数：

```text
Spark 主机:         172.16.100.143
仓库目录:           /root/spark-greenplum-connector
Connector JAR:      /root/spark-greenplum-connector/spark-greenplum-connector/target/spark-ymatrix-connector_2.12-3.1.jar

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

如果你的环境不同，只需要把下面命令中的这些值替换掉。

## 4. 执行前检查

### 4.1 登录服务器

直接复制执行：

```bash
ssh root@172.16.100.143
```

### 4.2 进入项目目录

直接复制执行：

```bash
cd /root/spark-greenplum-connector
```

### 4.3 检查 Spark 和 connector JAR

直接复制执行：

```bash
echo "SPARK_HOME=${SPARK_HOME:-/opt/spark}"
ls -lh /root/spark-greenplum-connector/spark-greenplum-connector/target/spark-ymatrix-connector_2.12-3.1.jar
```

如果 `SPARK_HOME` 没有设置，本文默认使用 `/opt/spark`。

### 4.4 设置 `SPARK_LOCAL_IP`

`gpfdist` 路径最关键的参数就是 `SPARK_LOCAL_IP`。它必须是 Greenplum segment 能回连的地址。

直接复制执行：

```bash
export SPARK_LOCAL_IP=172.16.100.143
echo "SPARK_LOCAL_IP=${SPARK_LOCAL_IP}"
```

如果 Greenplum segment 不能访问 `172.16.100.143`，请改成它们能访问到的那个地址。

### 4.5 预先创建 Greenplum 目标表

下面这个命令块可以直接复制执行。它会在 Greenplum 侧重建目标表：

```bash
PGPASSWORD='YMatrix@123' psql \
  -h 172.16.100.29 \
  -p 5432 \
  -U zhangchen \
  -d zhangchen <<'SQL'
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
SQL
```

如果当前机器没有 `psql`，可以把同一段 SQL 放到任意能连接 Greenplum 的客户端里执行。

## 5. 标准 spark-shell 启动命令

下面这条命令是本文方式一和方式二共用的启动命令，可以直接复制：

```bash
export SPARK_LOCAL_IP=172.16.100.143

${SPARK_HOME:-/opt/spark}/bin/spark-shell \
  --master local[8] \
  --driver-memory 16g \
  --conf "spark.driver.host=${SPARK_LOCAL_IP}" \
  --conf "spark.driver.bindAddress=0.0.0.0" \
  --conf "spark.local.ip=${SPARK_LOCAL_IP}" \
  --conf "spark.sql.shuffle.partitions=8" \
  --conf "spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog" \
  --conf "spark.sql.catalog.local.type=hadoop" \
  --conf "spark.sql.catalog.local.warehouse=/data/iceberg/warehouse" \
  --conf "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions" \
  --jars /root/spark-greenplum-connector/spark-greenplum-connector/target/spark-ymatrix-connector_2.12-3.1.jar
```

### 5.1 每个参数的含义

| 参数 | 含义 | 为什么这里必须带 |
|---|---|---|
| `--master local[8]` | 本机启动 8 个本地线程执行 Spark 作业 | 让本机具备并行读取和并行写入能力 |
| `--driver-memory 16g` | 给 Spark Driver 16GB 内存 | 宽表迁移时 schema、计划和 shuffle 都更稳 |
| `spark.driver.host=${SPARK_LOCAL_IP}` | Driver 对外通告自己的主机地址 | 让外部组件知道该回连哪个地址 |
| `spark.driver.bindAddress=0.0.0.0` | Driver 监听所有网卡 | 避免只绑到回环地址导致外部无法访问 |
| `spark.local.ip=${SPARK_LOCAL_IP}` | Spark 内部网络地址统一使用这个 IP | `gpfdist` 暴露地址会跟它强相关 |
| `spark.sql.shuffle.partitions=8` | Spark SQL shuffle 分区数 | 控制重分区和聚合的并行度 |
| `spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog` | 定义名为 `local` 的 Iceberg catalog | 这样才能访问 `local.test_db.iot_wide_0001` |
| `spark.sql.catalog.local.type=hadoop` | 指定 `local` catalog 使用 Hadoop 类型 | 当前仓库和本文都按这个方式读取 Iceberg |
| `spark.sql.catalog.local.warehouse=/data/iceberg/warehouse` | 指定 Iceberg warehouse 目录 | Spark 需要知道 Iceberg 元数据和数据文件在哪里 |
| `spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions` | 打开 Iceberg SQL 扩展 | 让 Spark 正确识别 Iceberg SQL / catalog 能力 |
| `--jars ...spark-ymatrix-connector_2.12-3.1.jar` | 把 connector JAR 加入 classpath | 没有它，Spark 不认识 `its-ymatrix` |

### 5.2 为什么 `SPARK_LOCAL_IP` 这么重要

因为 GPFDIST 不是单向 JDBC 写入。Spark 侧会临时拉起一个服务，Greenplum segment 要反向回连这个地址拉数据。

如果这个地址不可达，常见报错类似：

```text
ERROR: error when connecting to gpfdist http://172.16.100.143:33253/output.pipe, quit after 11 tries
(seg13 slice1 192.168.100.30:6005 pid=168824)
```

这时优先检查：

- `SPARK_LOCAL_IP` 是否设置成 segment 可达地址
- Spark 主机到 Greenplum 主库是否可达
- Greenplum segment 到 Spark 主机的端口是否放通

## 6. 方式一：spark-shell 交互式 Scala 写法

这是最直观、最容易排查问题的方式。

### 6.1 从头到尾的执行步骤

#### 第一步：登录并进入项目目录

```bash
ssh root@172.16.100.143
cd /root/spark-greenplum-connector
```

#### 第二步：确认 JAR 和网络地址

```bash
ls -lh /root/spark-greenplum-connector/spark-greenplum-connector/target/spark-ymatrix-connector_2.12-3.1.jar
export SPARK_LOCAL_IP=172.16.100.143
echo "SPARK_LOCAL_IP=${SPARK_LOCAL_IP}"
```

#### 第三步：准备目标表

直接执行第 4.5 节的 `psql` 建表命令。

#### 第四步：启动 spark-shell

直接执行第 5 节的完整启动命令。

#### 第五步：把下面整段 Scala 代码一次性复制到 spark-shell

```scala
import org.apache.spark.sql.functions.col

val icebergTable = "local.test_db.iot_wide_0001"
val gpUrl = "jdbc:postgresql://172.16.100.29:5432/zhangchen"
val gpUser = "zhangchen"
val gpPassword = "YMatrix@123"
val gpSchema = "public"
val gpTable = "iot_wide_0001"

val sourceDf = spark.table(icebergTable)

println(s"[schema]\n${sourceDf.schema.treeString}")
println(s"[source] rowCount=${sourceDf.count()}")

sourceDf
  .repartition(8, col("ingest_id"))
  .write
  .format("its-ymatrix")
  .option("url", gpUrl)
  .option("user", gpUser)
  .option("password", gpPassword)
  .option("dbschema", gpSchema)
  .option("dbtable", gpTable)
  .option("distributedby", "ingest_id")
  .option("network.timeout", "300s")
  .option("server.timeout", "300s")
  .option("dbmessages", "WARN")
  .mode("overwrite")
  .save()

spark.read
  .format("its-ymatrix")
  .option("url", gpUrl)
  .option("user", gpUser)
  .option("password", gpPassword)
  .option(
    "dbtable",
    "select count(*)::bigint as row_count, " +
      "coalesce(min(ingest_id), 0)::bigint as min_id, " +
      "coalesce(max(ingest_id), 0)::bigint as max_id " +
      "from public.iot_wide_0001"
  )
  .load()
  .show(false)
```

#### 第六步：退出 spark-shell

```scala
:quit
```

#### 第七步：在 Greenplum 侧做最终校验

```bash
PGPASSWORD='YMatrix@123' psql \
  -h 172.16.100.29 \
  -p 5432 \
  -U zhangchen \
  -d zhangchen <<'SQL'
SELECT count(*) FROM public.iot_wide_0001;
SELECT min(ingest_id), max(ingest_id) FROM public.iot_wide_0001;
SQL
```

### 6.2 方式一的成功标志

你通常会看到这些结果：

- `sourceDf.count()` 能正常输出源表行数
- `.save()` 阶段没有抛出 `gpfdist` 连接错误
- 最后 `show(false)` 能输出目标表 `row_count / min_id / max_id`
- `psql` 校验结果和源表统计一致

## 7. 方式二：使用 CREATE TEMPORARY VIEW + INSERT INTO 的 SQL 写法

这一种方式是本文修正后的 SQL 方式。

先把结论写清楚：

- 在你当前这套配置下，`CREATE TABLE ... USING its-ymatrix` 不能用
- 实际报错根因不是参数拼错，而是这条 DDL 会被 Iceberg 的 `SparkCatalog` 接管
- 当前环境里真正可落地的 SQL 方式，是 `CREATE TEMPORARY VIEW ... USING provider` 再 `INSERT INTO TABLE ... SELECT ...`

为什么 `CREATE TABLE` 不行：

- 当前 `spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog`
- `CREATE TABLE ... USING ...` 在这里会进入 catalog create-table 流程
- 从你实际报错堆栈可以看到，执行路径进入了 `org.apache.iceberg.spark.SparkCatalog.createTable`
- 它只接受自己支持的格式，因此会抛出 `Unsupported format in USING`

为什么 `CREATE TEMPORARY VIEW` 可以作为替代：

- `TEMPORARY VIEW` 不走持久 catalog 建表流程
- 它更接近 Spark 官方文档里 JDBC 数据源的 SQL 用法
- 我们把 provider 写成完整类名 `com.itsumma.gpconnector.GreenplumDataSource`，避免 `its-ymatrix` 这个带连字符的 short name 在 SQL DDL 中再次踩坑

### 7.1 这条方式里有两个“对象”

你需要明确区分下面两个名字：

- `gp_iot_wide_0001_sink`：Spark session 内的临时 sink view 名字
- `public.iot_wide_0001`：Greenplum 里的真实目标表

其中：

- `CREATE TEMPORARY VIEW gp_iot_wide_0001_sink USING com.itsumma.gpconnector.GreenplumDataSource` 是 Spark SQL 语句
- `OPTIONS (dbschema 'public', dbtable 'iot_wide_0001', ...)` 才是告诉 connector 最终往 Greenplum 哪张表写
- 因为 `TEMPORARY VIEW` 需要通过 provider 推断 schema，所以这条方式要求 Greenplum 目标表提前存在

### 7.2 从头到尾的执行步骤

#### 第一步：登录并进入项目目录

```bash
ssh root@172.16.100.143
cd /root/spark-greenplum-connector
```

#### 第二步：设置 `SPARK_LOCAL_IP`

```bash
export SPARK_LOCAL_IP=172.16.100.143
echo "SPARK_LOCAL_IP=${SPARK_LOCAL_IP}"
```

#### 第三步：先在 Greenplum 侧准备目标表

这一种 SQL 方式和方式一不同，它依赖 provider 先从目标表推断 schema，所以这里不能只 `DROP TABLE`，必须执行第 4.5 节里的完整建表命令，让 `public.iot_wide_0001` 先存在。

#### 第四步：启动 spark-shell

直接执行第 5 节的完整启动命令。

#### 第五步：把下面整段代码复制到 spark-shell

```scala
spark.sql("DROP VIEW IF EXISTS gp_iot_wide_0001_sink")

spark.sql(
  """
    |CREATE TEMPORARY VIEW gp_iot_wide_0001_sink
    |USING com.itsumma.gpconnector.GreenplumDataSource
    |OPTIONS (
    |  url 'jdbc:postgresql://172.16.100.29:5432/zhangchen',
    |  user 'zhangchen',
    |  password 'YMatrix@123',
    |  dbschema 'public',
    |  dbtable 'iot_wide_0001',
    |  distributedby 'ingest_id',
    |  network.timeout '300s',
    |  server.timeout '300s',
    |  dbmessages 'WARN'
    |)
    |""".stripMargin
)

spark.sql(
  """
    |INSERT INTO TABLE gp_iot_wide_0001_sink
    |SELECT *
    |FROM local.test_db.iot_wide_0001
    |""".stripMargin
)

spark.read
  .format("its-ymatrix")
  .option("url", "jdbc:postgresql://172.16.100.29:5432/zhangchen")
  .option("user", "zhangchen")
  .option("password", "YMatrix@123")
  .option(
    "dbtable",
    "select count(*)::bigint as row_count, " +
      "coalesce(min(ingest_id), 0)::bigint as min_id, " +
      "coalesce(max(ingest_id), 0)::bigint as max_id " +
      "from public.iot_wide_0001"
  )
  .load()
  .show(false)
```

#### 第六步：退出 spark-shell

```scala
:quit
```

#### 第七步：用 psql 校验

```bash
PGPASSWORD='YMatrix@123' psql \
  -h 172.16.100.29 \
  -p 5432 \
  -U zhangchen \
  -d zhangchen <<'SQL'
SELECT count(*) FROM public.iot_wide_0001;
SELECT min(ingest_id), max(ingest_id) FROM public.iot_wide_0001;
SQL
```

### 7.3 这一种方式的适用场景

适合下面场景：

- 你希望迁移动作尽量写成 SQL
- 你希望先把 sink view 在 Spark session 里注册出来，再统一用 `INSERT` 控制
- 你需要给使用 Spark SQL 的同学一套更接近 SQL 的操作路径

## 8. 方式三：使用仓库内置脚本

如果你不想手工进入 spark-shell 粘贴代码，直接用仓库脚本最快。

当前仓库的现成脚本是：

- [scripts/run-iceberg-iot-wide-0001-full-load.sh](/root/spark-greenplum-connector/scripts/run-iceberg-iot-wide-0001-full-load.sh)
- [scripts/migrate-iceberg-iot-wide-0001-full-load.scala](/root/spark-greenplum-connector/scripts/migrate-iceberg-iot-wide-0001-full-load.scala)

### 8.1 从头到尾的执行步骤

#### 第一步：登录并进入项目目录

```bash
ssh root@172.16.100.143
cd /root/spark-greenplum-connector
```

#### 第二步：检查并设置执行环境

```bash
ls -lh /root/spark-greenplum-connector/spark-greenplum-connector/target/spark-ymatrix-connector_2.12-3.1.jar
export SPARK_LOCAL_IP=172.16.100.143
chmod +x /root/spark-greenplum-connector/scripts/run-iceberg-iot-wide-0001-full-load.sh
```

#### 第三步：执行脚本

```bash
/root/spark-greenplum-connector/scripts/run-iceberg-iot-wide-0001-full-load.sh
```

#### 第四步：执行结束后校验

```bash
PGPASSWORD='YMatrix@123' psql \
  -h 172.16.100.29 \
  -p 5432 \
  -U zhangchen \
  -d zhangchen <<'SQL'
SELECT count(*) FROM public.iot_wide_0001;
SELECT min(ingest_id), max(ingest_id) FROM public.iot_wide_0001;
SQL
```

### 8.2 这套脚本做了什么

这套脚本会自动完成下面这些动作：

1. 启动 `spark-shell`
2. 读取 `local.test_db.iot_wide_0001`
3. 打印源表 schema
4. 统计源表 `count(*) / min(ingest_id) / max(ingest_id)`
5. 探测 Greenplum JDBC 连通性
6. 使用 `its-ymatrix` connector 执行 `overwrite`
7. 回查目标表并比较结果

### 8.3 脚本成功时通常能看到这些日志

```text
[schema] ...
[source] rowCount=...
[probe] Greenplum JDBC probe passed
[target] rowCount=...
[done] Successfully migrated ... rows from local.test_db.iot_wide_0001 to public.iot_wide_0001
```

## 9. 三种方式如何选择

| 方式 | 推荐场景 | 特点 |
|---|---|---|
| 方式一：spark-shell Scala | 你要边跑边看、边查边调 | 最容易排障 |
| 方式二：TEMP VIEW + INSERT | 你希望迁移逻辑尽量用 SQL 表达 | 当前环境里可落地的 SQL 写法 |
| 方式三：仓库脚本 | 你要快速跑通一次标准全量迁移 | 最省事、最接近现成入口 |

## 10. 常见问题

### 10.1 为什么命令里明明是 GPFDIST，还必须带 JDBC URL

因为当前 connector 的职责不是只有“传输数据”。

它还要通过 JDBC 去做：

- 建表或检查表结构
- 获取目标表元数据
- 校验写入结果

所以 `url / user / password` 是必填参数。

### 10.2 为什么本文一直强调 `SPARK_LOCAL_IP`

因为 Greenplum segment 需要按这个地址回连 Spark 暴露出来的 `gpfdist` 服务。

如果这个地址填错了，常见现象是：

- 作业长时间卡住
- 最终报 `error when connecting to gpfdist`
- 日志里能看到 segment IP，但连不上 Spark 暴露端口

### 10.3 如果当前机器没有 `psql` 怎么办

你可以：

- 在任意能连接 Greenplum 的机器上执行本文给出的 SQL
- 或者直接使用方式一、方式二里的 Spark 回查代码做验证

### 10.4 如果想固定 gpfdist 端口怎么办

可以在 connector 选项里额外指定：

```scala
.option("server.port", "43000")
```

或者在 SQL `OPTIONS` 里加上：

```sql
server.port '43000'
```

这对排查网络问题很有帮助。

## 11. 最短可复制版本

如果你只想要一套最短命令，优先使用仓库脚本：

```bash
ssh root@172.16.100.143
cd /root/spark-greenplum-connector
export SPARK_LOCAL_IP=172.16.100.143
chmod +x /root/spark-greenplum-connector/scripts/run-iceberg-iot-wide-0001-full-load.sh
/root/spark-greenplum-connector/scripts/run-iceberg-iot-wide-0001-full-load.sh
```

跑完再校验：

```bash
PGPASSWORD='YMatrix@123' psql \
  -h 172.16.100.29 \
  -p 5432 \
  -U zhangchen \
  -d zhangchen <<'SQL'
SELECT count(*) FROM public.iot_wide_0001;
SELECT min(ingest_id), max(ingest_id) FROM public.iot_wide_0001;
SQL
```
