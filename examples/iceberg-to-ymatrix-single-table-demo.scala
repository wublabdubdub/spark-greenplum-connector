// 导入列函数，用于后续从查询结果中读取 row_count 字段。
import org.apache.spark.sql.functions.col

/**
 * 最小示例：
 * 把一个 Iceberg 表复制到一个 YMatrix 表。
 *
 * 这个脚本适合在 spark-shell 中直接 :load 执行。
 *
 * 运行前请确保：
 * 1. Spark 里已经配置好 Iceberg catalog
 * 2. connector JAR 已通过 --jars 加入
 *
 * 示例命令：
 *
 * spark-shell \
 *   --master local[4] \
 *   --driver-memory 8g \
 *   --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
 *   --conf spark.sql.catalog.local.type=hadoop \
 *   --conf spark.sql.catalog.local.warehouse=/tmp/iceberg-warehouse \
 *   --jars ./spark-ymatrix-connector_2.12-3.1.jar
 *
 * scala> :load examples/iceberg-to-ymatrix-single-table-demo.scala
 */

// 定义要读取的 Iceberg 源表全名。
val icebergTable = "local.demo_db.orders"

// 定义 YMatrix JDBC 地址。
val ymatrixUrl = "jdbc:postgresql://127.0.0.1:5432/demo"
// 定义 YMatrix 用户名。
val ymatrixUser = "ymatrix_user"
// 定义 YMatrix 密码。
val ymatrixPassword = "ymatrix_password"
// 定义 YMatrix 目标 schema。
val ymatrixSchema = "public"
// 定义 YMatrix 目标表名。
val ymatrixTable = "orders_from_iceberg"

// 定义写入模式，支持 append 或 overwrite。
val writeMode = "append"
// 定义读取源表后重新分区时使用的分区数。
val writePartitions = 4
// 定义 YMatrix 目标表分布键。
val ymatrixDistributedBy = "id"
// 定义 connector 网络超时时间。
val ymatrixNetworkTimeout = "120s"
// 定义 connector 服务端超时时间。
val ymatrixServerTimeout = "120s"
// 定义数据库消息输出级别。
val ymatrixDbMessages = "WARN"

// 校验写入模式是否合法。
require(Set("append", "overwrite").contains(writeMode), "writeMode must be append or overwrite")
// 校验分区数必须为正数。
require(writePartitions > 0, "writePartitions must be positive")

// 从 Iceberg 读取源表，并按指定分区数重分区。
val sourceDf = spark.table(icebergTable).repartition(writePartitions)

// 统计源表行数，便于后续打印和空表判断。
val sourceCount = sourceDf.count()
// 打印源表基本信息。
println(s"[source] icebergTable=${icebergTable}, rows=${sourceCount}")

// 如果源表为空，则直接跳过写入。
if (sourceCount == 0L) {
  // 打印跳过信息。
  println(s"[skip] ${icebergTable} is empty")
} else {
  // 打印源表字段列表，便于快速确认 schema。
  println(s"[schema] source columns: ${sourceDf.columns.mkString(", ")}")

  // 把源数据写入 YMatrix 目标表。
  sourceDf.write
    // 指定使用 its-ymatrix connector。
    .format("its-ymatrix")
    // 指定 YMatrix JDBC 地址。
    .option("url", ymatrixUrl)
    // 指定 YMatrix 用户名。
    .option("user", ymatrixUser)
    // 指定 YMatrix 密码。
    .option("password", ymatrixPassword)
    // 指定 YMatrix schema。
    .option("dbschema", ymatrixSchema)
    // 指定 YMatrix 目标表名。
    .option("dbtable", ymatrixTable)
    // 指定目标表分布键。
    .option("distributedby", ymatrixDistributedBy)
    // 指定网络超时。
    .option("network.timeout", ymatrixNetworkTimeout)
    // 指定服务端超时。
    .option("server.timeout", ymatrixServerTimeout)
    // 指定数据库消息级别。
    .option("dbmessages", ymatrixDbMessages)
    // 使用上面定义的写入模式。
    .mode(writeMode)
    // 真正触发保存动作。
    .save()

  // 回查 YMatrix 目标表行数，用于简单校验。
  val ymatrixCount = spark.read
    // 通过 connector 读取 YMatrix 查询结果。
    .format("its-ymatrix")
    // 指定 YMatrix JDBC 地址。
    .option("url", ymatrixUrl)
    // 指定 YMatrix 用户名。
    .option("user", ymatrixUser)
    // 指定 YMatrix 密码。
    .option("password", ymatrixPassword)
    // 执行 count 查询并返回单行结果。
    .option("dbtable", s"select count(*)::bigint as row_count from ${ymatrixSchema}.${ymatrixTable}")
    // 加载查询结果。
    .load()
    // 选出 row_count 列并转成 long。
    .select(col("row_count").cast("long"))
    // 取第一行。
    .head()
    // 读取 long 值。
    .getLong(0)

  // 打印迁移完成日志。
  println(s"[done] copied ${sourceCount} rows from ${icebergTable} to ${ymatrixSchema}.${ymatrixTable}")
  // 打印目标表回查行数。
  println(s"[verify] ymatrix rows=${ymatrixCount}")
}
