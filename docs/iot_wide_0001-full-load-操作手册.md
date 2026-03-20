# iot_wide_0001 Iceberg 到 Greenplum 全量迁移操作手册

## 1. 目标

将本机 Iceberg 表 `local.test_db.iot_wide_0001` 全量迁移到 Greenplum 表 `public.iot_wide_0001`。

本手册对应两套本地脚本：

- `scripts/migrate-iceberg-iot-wide-0001-full-load.scala`
- `scripts/run-iceberg-iot-wide-0001-full-load.sh`
- `scripts/migrate-iceberg-iot-wide-0001-full-load-jdbc.scala`
- `scripts/run-iceberg-iot-wide-0001-full-load-jdbc.sh`

其中：

- `full-load.sh` 是 connector + GPFDIST 方案。
- `full-load-jdbc.sh` 是不依赖 GPFDIST 回连的 Spark SQL + JDBC 方案。

基于本机实际执行结果，当前机器只有 `172.16.100.143` 地址，而 Greenplum 报错来自 `192.168.100.30` segment，无法回连到 `172.16.100.143` 的 GPFDIST 服务。因此当前“完整可行”的执行方案以 JDBC 版本为准。

## 2. 固定参数

当前脚本已经固化了以下参数，无需再手改：

```text
Iceberg warehouse: /data/iceberg/warehouse
Iceberg table:     local.test_db.iot_wide_0001
Greenplum URL:     jdbc:postgresql://172.16.100.29:5432/zhangchen
Greenplum user:    zhangchen
Greenplum schema:  public
Greenplum table:   iot_wide_0001
分布键:             ingest_id
写入模式:           重建目标表后全量导入
```

说明：

- JDBC 方案会先 `drop table if exists`，再按源表结构重建目标表。
- 重建时会显式带上 `distributed by (ingest_id)`。
- 迁移主流程不是 `its-ymatrix` connector 写入，而是 Spark 内置 `jdbc` 数据源配合 SQL：
  - `create table ... using jdbc options (...)`
  - `insert into ... select * from local.test_db.iot_wide_0001`
- 作业结束后会自动校验源表与目标表的行数、`min(ingest_id)`、`max(ingest_id)`。

当前脚本的实际迁移路径可以概括成：

```text
Iceberg(local.test_db.iot_wide_0001)
  -> Spark SQL
  -> JDBC sink table(using jdbc)
  -> Greenplum(public.iot_wide_0001)
```

这条链路只要求 Spark 主动连接 `172.16.100.29:5432`，不要求 Greenplum segment 反向回连当前机器，因此不会再触发之前的 GPFDIST 回连失败问题。

## 3. 登录后执行步骤

### 3.1 登录服务器

```bash
ssh root@172.16.100.143
```

如果你已经在机器上，可以直接进入仓库目录。

### 3.2 进入项目目录

```bash
cd /root/spark-greenplum-connector
```

### 3.3 确认 Spark 和 JDBC 驱动 JAR

```bash
echo "$SPARK_HOME"
ls -lh /root/spark-greenplum-connector/spark-greenplum-connector/target/spark-ymatrix-connector_2.12-3.1.jar
```

如果 `SPARK_HOME` 为空，脚本会默认使用 `/opt/spark`。

说明：

- 当前 SQL + JDBC 方案不调用 `its-ymatrix` sink。
- 这里继续复用仓库里已有的 JAR，主要是为了给 Spark 提供 PostgreSQL JDBC Driver。
- 如果这个 JAR 不存在，启动脚本会自动退回到 `--packages org.postgresql:postgresql:42.7.2`。

### 3.4 赋予执行权限

```bash
chmod +x /root/spark-greenplum-connector/scripts/run-iceberg-iot-wide-0001-full-load.sh
chmod +x /root/spark-greenplum-connector/scripts/run-iceberg-iot-wide-0001-full-load-jdbc.sh
```

### 3.5 执行当前可行的全量迁移方案

```bash
/root/spark-greenplum-connector/scripts/run-iceberg-iot-wide-0001-full-load-jdbc.sh
```

如果后续网络打通，也可以尝试 connector 方案：

```bash
/root/spark-greenplum-connector/scripts/run-iceberg-iot-wide-0001-full-load.sh
```

## 4. 成功标志

日志中看到以下关键字，说明迁移成功：

```text
[probe] Greenplum JDBC probe passed
[ddl] Recreated public.iot_wide_0001 with DISTRIBUTED BY (ingest_id)
[ddl] Registered Spark SQL JDBC sink table default.gp_iot_wide_0001_jdbc_sink -> public.iot_wide_0001
[sql] Running Spark SQL insert-select from local.test_db.iot_wide_0001 to default.gp_iot_wide_0001_jdbc_sink
[source] rowCount=...
[target] rowCount=...
[done] Successfully migrated ... rows from local.test_db.iot_wide_0001 to public.iot_wide_0001 through Spark SQL + JDBC
```

## 5. 失败时优先检查

### 5.1 Iceberg 表不存在或 catalog 未生效

检查脚本中的 catalog 配置是否仍为：

```text
spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.local.type=hadoop
spark.sql.catalog.local.warehouse=/data/iceberg/warehouse
```

### 5.2 Greenplum 无法连接

检查网络连通性和账号密码是否仍然有效：

```text
jdbc:postgresql://172.16.100.29:5432/zhangchen
user=zhangchen
schema=public
table=iot_wide_0001
```

### 5.3 为什么不直接用 connector 版本

本机当前只有：

```text
172.16.100.143
```

但本次实际报错来自：

```text
seg13 slice1 192.168.100.30:6005
```

也就是 Greenplum segment 位于 `192.168.100.x` 网段，当前无法回连本机暴露的 `172.16.100.143:gpfdist_port`，所以 connector 方案会失败。

而当前 SQL + JDBC 方案之所以可行，是因为：

- Spark 只需要主动连接 `172.16.100.29:5432`
- Greenplum segment 不需要再回连 Spark 本机
- 所以网络依赖从“双向可达”降成了“Spark 到 Greenplum 单向可达”

如果后续要恢复 connector 方案，需要至少满足以下任一条件：

- Spark 主机增加一个 Greenplum segment 可达的 `192.168.100.x` 地址，并设置 `SPARK_LOCAL_IP`
- 网络侧打通 `192.168.100.x -> 172.16.100.143` 的访问
- 在与 Greenplum segment 同网段的机器上运行 Spark 作业

## 6. 补充校验

如果你后续安装了 `psql`，可以再补做一次数据库侧校验：

```sql
select count(*) from public.iot_wide_0001;
select min(ingest_id), max(ingest_id) from public.iot_wide_0001;
```

如果没有 `psql`，当前 Scala 脚本已经在作业结束时通过 JDBC 自动执行了同样的校验。
