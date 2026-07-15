# CN2 端口平移功能使用手册

## 适用场景

MDB/YDB segment 无法直接访问 Spark executor，需要通过 CN2 和 Nginx 访问；不同 Spark 机器使用相同的本地端口段，但对外发布端口段不同。

## 参数示例

在原有 `spark.read` 配置中增加 `server.publish.mapping`：

```scala
val df = spark.read
  .format("its-ymatrix")
  .option("url", "jdbc:postgresql://<MDB地址>:5432/<数据库>")
  .option("user", "<用户名>")
  .option("password", "<密码>")
  .option("dbtable", "<表名或查询SQL>")
  .option(
    "server.publish.mapping",
    Seq(
      "172.26.32.28=10.150.0.5:20101-20110->20111-20120",
      "172.26.32.113=10.150.0.5:20111-20120->20111-20120",
      "172.26.32.114=10.150.0.5:20121-20130->20111-20120"
    ).mkString(",")
  )
  .load()
```

该参数同样适用于 `spark.write.format("its-ymatrix")`。

## 参数格式

```text
Spark内网IP=CN2发布IP:发布端口段->Spark本地端口段
```

例如：

```text
172.26.32.28=10.150.0.5:20101-20110->20111-20120
```

表示 Spark 在 `172.26.32.28:20111-20120` 中选择端口，对 MDB/YDB 发布为 `10.150.0.5:20101-20110`。如果 Spark 选中本地端口 `20114`，MDB/YDB 将访问 `10.150.0.5:20104`。

## 注意事项

- 发布端口段和本地端口段包含的端口数量必须相同。
- 配置该参数后，不要再配置 `server.port`。
- 参数中的 Spark 内网 IP 必须与 executor 实际探测到的 IP 一致。
- 当前每台机器配置 10 个本地端口，端口全部被占用时任务会失败，不会改用随机端口。
- 多条映射必须通过 `Seq(...).mkString(",")` 拼成一个字符串传给 `.option`。
