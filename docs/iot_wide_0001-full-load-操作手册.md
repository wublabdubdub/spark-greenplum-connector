# iot_wide_0001 Iceberg 到 Greenplum 全量迁移操作手册

## 1. 目标

将本机 Iceberg 表 `local.test_db.iot_wide_0001` 通过 `its-ymatrix` connector 的 `gpfdist` 路径，全量迁移到 Greenplum 表 `public.iot_wide_0001`。

本手册对应的仓库材料只有两份：

- `scripts/migrate-iceberg-iot-wide-0001-full-load.scala`
- `scripts/run-iceberg-iot-wide-0001-full-load.sh`

迁移链路如下：

```text
Iceberg(local.test_db.iot_wide_0001)
  -> Spark DataFrame
  -> format("its-ymatrix")
  -> GPFDIST + JDBC metadata
  -> Greenplum(public.iot_wide_0001)
```

## 2. 固定参数

当前脚本已经固化以下参数：

```text
Iceberg warehouse: /data/iceberg/warehouse
Iceberg table:     local.test_db.iot_wide_0001
Greenplum URL:     jdbc:postgresql://172.16.100.29:5432/zhangchen
Greenplum user:    zhangchen
Greenplum schema:  public
Greenplum table:   iot_wide_0001
分布键:             ingest_id
写入模式:           overwrite
```

执行时会自动完成这些动作：

- 校验源表 `count(*)`、`min(ingest_id)`、`max(ingest_id)`
- 探测 Greenplum JDBC 连通性
- 用 `its-ymatrix` connector 写入目标表
- 回查目标表并校验行数与主键范围

## 3. 执行步骤

### 3.1 登录并进入项目目录

```bash
ssh root@172.16.100.143
cd /root/spark-greenplum-connector
```

### 3.2 确认 Spark 与 connector JAR

```bash
echo "$SPARK_HOME"
ls -lh /root/spark-greenplum-connector/spark-greenplum-connector/target/spark-ymatrix-connector_2.12-3.1.jar
```

如果 `SPARK_HOME` 为空，脚本会默认使用 `/opt/spark`。

### 3.3 确认 `SPARK_LOCAL_IP`

`gpfdist` 路径要求 Greenplum segment 能回连 Spark 暴露出来的地址。建议在执行前显式设置：

```bash
export SPARK_LOCAL_IP=172.16.100.143
```

如果你有一个对 Greenplum segment 更可达的地址，请改成那个地址。

### 3.4 赋予执行权限并启动

```bash
chmod +x /root/spark-greenplum-connector/scripts/run-iceberg-iot-wide-0001-full-load.sh
/root/spark-greenplum-connector/scripts/run-iceberg-iot-wide-0001-full-load.sh
```

## 4. 成功标志

日志中出现以下关键内容，说明迁移已经跑通：

```text
[probe] Greenplum JDBC probe passed
[source] rowCount=...
[target] rowCount=...
[done] Successfully migrated ... rows from local.test_db.iot_wide_0001 to public.iot_wide_0001
```

## 5. 失败时优先检查

### 5.1 Iceberg catalog 配置

检查脚本启动参数里是否仍然包含：

```text
spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.local.type=hadoop
spark.sql.catalog.local.warehouse=/data/iceberg/warehouse
```

### 5.2 Greenplum 基础连通性

确认以下连接参数仍然有效：

```text
jdbc:postgresql://172.16.100.29:5432/zhangchen
user=zhangchen
schema=public
table=iot_wide_0001
```

### 5.3 GPFDIST 回连能力

`gpfdist` 是双向链路。除了 Spark 能访问 Greenplum，还要求 Greenplum segment 能访问 `SPARK_LOCAL_IP` 对应地址。

如果日志里出现类似报错：

```text
ERROR: error when connecting to gpfdist http://172.16.100.143:33253/output.pipe, quit after 11 tries
(seg13 slice1 192.168.100.30:6005 pid=168824)
```

通常表示：

- Spark 已经拉起临时 `gpfdist` 服务
- Greenplum segment 无法回连这个地址
- 作业会在网络超时后失败

这时应优先检查：

- `SPARK_LOCAL_IP` 是否设置成 segment 可达地址
- Spark 所在主机是否有 Greenplum 网段可达 IP
- 防火墙、路由或安全组是否放通 segment 到 Spark 的回连

## 6. 补充校验

如果你安装了 `psql`，可以额外在数据库侧检查：

```sql
select count(*) from public.iot_wide_0001;
select min(ingest_id), max(ingest_id) from public.iot_wide_0001;
```

即使没有 `psql`，Scala 脚本也会在作业结束前通过 JDBC 自动做同样的结果校验。
