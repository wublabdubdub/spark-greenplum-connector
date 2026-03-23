// 导入 JDBC DriverManager，用于建立到 Greenplum 的 JDBC 连接。
import java.sql.DriverManager

// 导入 Spark SQL 常用函数，供聚合、列引用和常量构造使用。
import org.apache.spark.sql.functions._

// 导入 NonFatal，用来捕获非致命异常并统一设置退出码。
import scala.util.control.NonFatal

// 定义要读取的 Iceberg 源表全名。
val icebergTable = "local.test_db.iot_wide_0001"

// 定义 Greenplum JDBC 连接地址。
val gpUrl = "jdbc:postgresql://172.16.100.29:5432/zhangchen"
// 定义 Greenplum 登录用户名。
val gpUser = "zhangchen"
// 定义 Greenplum 登录密码。
val gpPassword = "YMatrix@123"
// 定义 Greenplum 目标 schema。
val gpSchema = "public"
// 定义 Greenplum 目标表名。
val gpTable = "iot_wide_0001"

// 定义写入阶段按分布键重分区时使用的分区数。
val writePartitions = 8
// 定义 Spark SQL shuffle 阶段使用的分区数。
val shufflePartitions = 8
// 定义 Greenplum 目标表使用的分布键列名。
val distributedBy = "ingest_id"
// 定义 connector 侧网络超时时间。
val networkTimeout = "300s"
// 定义 connector 侧服务端超时时间。
val serverTimeout = "300s"
// 定义 connector 输出的数据库消息级别。
val dbMessages = "WARN"

// 定义一个简单的统计结果结构，保存行数和 ingest_id 范围。
case class TableProfile(rowCount: Long, minIngestId: Long, maxIngestId: Long)

// 将普通标识符转成带双引号的 SQL 标识符，避免大小写和关键字问题。
def quoteIdent(ident: String): String = "\"" + ident.replace("\"", "\"\"") + "\""

// 查询 Greenplum 目标表的行数和 ingest_id 范围。
def queryGpProfile(): TableProfile = {
  // 构造用于统计目标表的 SQL。
  val sql =
    // 使用字符串插值拼出目标表统计语句。
    s"""
       |select
       |  count(*)::bigint as row_count,
       |  coalesce(min(ingest_id), 0)::bigint as min_ingest_id,
       |  coalesce(max(ingest_id), 0)::bigint as max_ingest_id
       |from ${quoteIdent(gpSchema)}.${quoteIdent(gpTable)}
       |""".stripMargin

  // 建立到 Greenplum 的 JDBC 连接。
  val conn = DriverManager.getConnection(gpUrl, gpUser, gpPassword)
  // 使用 try/finally 确保连接最终会被关闭。
  try {
    // 基于 SQL 创建预编译语句。
    val stmt = conn.prepareStatement(sql)
    // 使用 try/finally 确保语句对象最终会被关闭。
    try {
      // 执行查询并拿到结果集。
      val rs = stmt.executeQuery()
      // 使用 try/finally 确保结果集最终会被关闭。
      try {
        // 将结果集游标移动到第一行。
        rs.next()
        // 读取统计列并组装成 TableProfile 返回。
        TableProfile(rs.getLong("row_count"), rs.getLong("min_ingest_id"), rs.getLong("max_ingest_id"))
      // 进入 finally 块关闭结果集。
      } finally {
        // 关闭结果集，释放数据库资源。
        rs.close()
      }
    // 进入 finally 块关闭 PreparedStatement。
    } finally {
      // 关闭语句对象，释放数据库资源。
      stmt.close()
    }
  // 进入 finally 块关闭连接。
  } finally {
    // 关闭 JDBC 连接，释放数据库连接资源。
    conn.close()
  }
}

// 对 Greenplum 做一个最简单的连通性探测。
def probeGp(): Unit = {
  // 建立到 Greenplum 的 JDBC 连接。
  val conn = DriverManager.getConnection(gpUrl, gpUser, gpPassword)
  // 使用 try/finally 确保连接会被关闭。
  try {
    // 构造最简单的探测 SQL。
    val stmt = conn.prepareStatement("select 1")
    // 使用 try/finally 确保语句对象会被关闭。
    try {
      // 执行探测 SQL 并获取结果集。
      val rs = stmt.executeQuery()
      // 使用 try/finally 确保结果集会被关闭。
      try {
        // 要求返回一行且第一列为 1，否则认为探测失败。
        require(rs.next() && rs.getInt(1) == 1, "Greenplum JDBC probe failed")
      // 进入 finally 块关闭结果集。
      } finally {
        // 关闭结果集。
        rs.close()
      }
    // 进入 finally 块关闭 PreparedStatement。
    } finally {
      // 关闭语句对象。
      stmt.close()
    }
  // 进入 finally 块关闭连接。
  } finally {
    // 关闭 JDBC 连接。
    conn.close()
  }
}

// 统计 Iceberg 源表的行数和 ingest_id 范围。
def profileIcebergTable(): TableProfile = {
  // 从 Iceberg 表读取数据并执行聚合统计。
  val metrics = spark.table(icebergTable)
    // 对整表执行聚合，得到行数和 ingest_id 范围。
    .agg(
      // 统计总行数并命名为 row_count。
      count(lit(1)).cast("long").as("row_count"),
      // 统计最小 ingest_id，为空时回填 0。
      coalesce(min(col("ingest_id")), lit(0L)).cast("long").as("min_ingest_id"),
      // 统计最大 ingest_id，为空时回填 0。
      coalesce(max(col("ingest_id")), lit(0L)).cast("long").as("max_ingest_id")
    )
    // 取聚合结果中的第一行。
    .head()

  // 将 Spark Row 转换成更易用的 TableProfile。
  TableProfile(
    // 读取统计结果里的行数字段。
    metrics.getAs[Long]("row_count"),
    // 读取统计结果里的最小 ingest_id 字段。
    metrics.getAs[Long]("min_ingest_id"),
    // 读取统计结果里的最大 ingest_id 字段。
    metrics.getAs[Long]("max_ingest_id")
  )
}

// 设置 Spark SQL 的 shuffle 分区数，使执行计划和脚本参数保持一致。
spark.conf.set("spark.sql.shuffle.partitions", shufflePartitions.toString)

// 执行主流程，并把成功或失败映射成退出码。
val exitCode = try {
  // 打印当前脚本的关键配置信息，便于排障。
  println(
    // 用多行字符串拼出配置日志。
    s"""
       |[config] icebergTable=${icebergTable}
       |[config] gpTarget=${gpSchema}.${gpTable}
       |[config] writePartitions=${writePartitions}
       |[config] shufflePartitions=${shufflePartitions}
       |[config] distributedBy=${distributedBy}
       |""".stripMargin
  )

  // 从 Iceberg 表读取源数据。
  val sourceDf = spark.table(icebergTable)
  // 打印源表 schema，方便观察字段结构。
  println(s"[schema] ${sourceDf.schema.treeString}")

  // 统计源表的行数和 ingest_id 范围。
  val sourceProfile = profileIcebergTable()
  // 打印源表统计信息。
  println(
    // 拼接源表统计日志文本。
    s"[source] rowCount=${sourceProfile.rowCount}, " +
      // 继续拼接 ingest_id 范围信息。
      s"minIngestId=${sourceProfile.minIngestId}, maxIngestId=${sourceProfile.maxIngestId}"
  )

  // 要求源表不能为空，否则直接失败退出。
  require(sourceProfile.rowCount > 0L, s"${icebergTable} is empty")

  // 先做一次 Greenplum 探测，尽早发现连接问题。
  probeGp()
  // 打印探测通过日志。
  println("[probe] Greenplum JDBC probe passed")

  // 对源数据按分布键重分区后写入 Greenplum。
  sourceDf
    // 按 ingest_id 进行重分区，帮助数据分布更均匀。
    .repartition(writePartitions, col("ingest_id"))
    // 进入 DataFrameWriter。
    .write
    // 指定使用 its-ymatrix connector。
    .format("its-ymatrix")
    // 传入 Greenplum JDBC 地址。
    .option("url", gpUrl)
    // 传入 Greenplum 用户名。
    .option("user", gpUser)
    // 传入 Greenplum 密码。
    .option("password", gpPassword)
    // 指定目标 schema。
    .option("dbschema", gpSchema)
    // 指定目标表名。
    .option("dbtable", gpTable)
    // 指定 Greenplum 分布键。
    .option("distributedby", distributedBy)
    // 指定网络超时时间。
    .option("network.timeout", networkTimeout)
    // 指定服务端超时时间。
    .option("server.timeout", serverTimeout)
    // 指定数据库消息输出级别。
    .option("dbmessages", dbMessages)
    // 使用 overwrite 模式覆盖目标表数据。
    .mode("overwrite")
    // 真正触发保存动作。
    .save()

  // 写入完成后查询目标表统计信息。
  val targetProfile = queryGpProfile()
  // 打印目标表统计信息。
  println(
    // 拼接目标表的行数日志。
    s"[target] rowCount=${targetProfile.rowCount}, " +
      // 继续拼接目标表 ingest_id 范围日志。
      s"minIngestId=${targetProfile.minIngestId}, maxIngestId=${targetProfile.maxIngestId}"
  )

  // 校验目标表行数必须和源表完全一致。
  require(
    // 比较目标表和源表的行数。
    targetProfile.rowCount == sourceProfile.rowCount,
    // 构造行数不一致时的错误消息。
    s"Row count mismatch: source=${sourceProfile.rowCount}, target=${targetProfile.rowCount}"
  )
  // 校验目标表的 ingest_id 范围必须和源表一致。
  require(
    // 要求最小值和最大值都完全相等。
    targetProfile.minIngestId == sourceProfile.minIngestId &&
      // 继续校验最大 ingest_id。
      targetProfile.maxIngestId == sourceProfile.maxIngestId,
    // 构造 ingest_id 范围不一致时的错误消息。
    s"Ingest range mismatch: source=[${sourceProfile.minIngestId}, ${sourceProfile.maxIngestId}], " +
      // 继续拼接目标表范围信息。
      s"target=[${targetProfile.minIngestId}, ${targetProfile.maxIngestId}]"
  )

  // 打印迁移成功日志。
  println(
    // 拼接成功日志的前半部分。
    s"[done] Successfully migrated ${sourceProfile.rowCount} rows from ${icebergTable} " +
      // 拼接成功日志的目标表信息。
      s"to ${gpSchema}.${gpTable}"
  )
  // 返回 0 表示脚本成功。
  0
// 如果主流程里抛出异常，则进入 catch 分支。
} catch {
  // 捕获非致命异常并统一处理。
  case NonFatal(e) =>
    // 打印异常堆栈，方便排查问题。
    e.printStackTrace()
    // 返回 1 表示脚本失败。
    1
// 无论成功失败，都会进入 finally 分支。
} finally {
  // 停止 SparkSession，释放资源。
  spark.stop()
}

// 使用退出码结束当前 spark-shell 脚本进程。
System.exit(exitCode)
