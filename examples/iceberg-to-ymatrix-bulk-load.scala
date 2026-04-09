// 导入 DriverManager，用于直接通过 JDBC 访问 YMatrix 做元数据检查。
import java.sql.DriverManager

// 导入 DataFrame 类型，供函数签名使用。
import org.apache.spark.sql.DataFrame
// 导入 Spark SQL 常用函数。
import org.apache.spark.sql.functions._

/**
 * 这个示例建议通过 spark-shell 运行。
 *
 * 运行前请确保：
 * 1. Iceberg catalog 已在 Spark 中配置好
 * 2. connector JAR 已通过 --jars 带上
 *
 * 示例：
 *
 * export SPARK_LOCAL_IP=172.16.100.32
 * spark-shell \
 *   --master local[8] \
 *   --driver-memory 16g \
 *   --conf spark.driver.host=172.16.100.32 \
 *   --conf spark.driver.bindAddress=0.0.0.0 \
 *   --conf spark.local.ip=172.16.100.32 \
 *   --conf spark.sql.shuffle.partitions=8 \
 *   --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
 *   --conf spark.sql.catalog.local.type=hadoop \
 *   --conf spark.sql.catalog.local.warehouse=/tmp/iceberg-warehouse \
 *   --jars ./spark-ymatrix-connector_2.12-3.1.jar
 *
 * scala> :load examples/iceberg-to-ymatrix-bulk-load.scala
 *
 * 脚本会按表逐个把历史数据从 Iceberg 拷贝到 YMatrix。
 * 它通过比较源端和目标端的 max(ingest_id) 支持断点续跑。
 */

// 导入 implicits，供 .as[Long] 使用。
import spark.implicits._

// 定义 Iceberg namespace。
val icebergNamespace = "local.test_db"
// 定义 Iceberg 表名前缀。
val icebergTablePrefix = "iot_wide_"

// 定义 YMatrix JDBC 地址。
val ymatrixUrl = "jdbc:postgresql://172.16.100.29:5432/zhangchen"
// 定义 YMatrix 用户名。
val ymatrixUser = "zhangchen"
// 定义 YMatrix 密码。
val ymatrixPassword = "YMatrix@123"
// 定义 YMatrix schema。
val ymatrixSchema = "public"
// 定义 YMatrix 目标表名前缀。
val ymatrixTablePrefix = "iot_wide_"

// 定义起始表号。
val tableStartIndex = 1
// 定义要处理的表数量。
val tableCount = 10
// 定义每个批次按 ingest_id 划分的行数区间大小。
val rowsPerBatch = 200000L
// 定义 Spark 侧写入分区数。
val writePartitions = 8

// 定义 connector 网络超时。
val ymatrixNetworkTimeout = "120s"
// 定义 connector 服务端超时。
val ymatrixServerTimeout = "120s"
// 定义数据库消息级别。
val ymatrixDbMessages = "WARN"
// 定义 YMatrix 目标表分布键。
val ymatrixDistributedBy = "ingest_id"

// 校验表数量必须为正数。
require(tableCount > 0, "tableCount must be positive")
// 校验批次大小必须为正数。
require(rowsPerBatch > 0, "rowsPerBatch must be positive")
// 校验写入分区数必须为正数。
require(writePartitions > 0, "writePartitions must be positive")

// 定义宽表的字段清单，用于 select 时保持列顺序稳定。
val wideColumns = Seq(
  "ingest_id",
  "event_id",
  "device_id",
  "tenant_id",
  "site_id",
  "line_id",
  "region_code",
  "event_time",
  "event_date",
  "event_minute",
  "status_code",
  "alarm_level",
  "metric_01",
  "metric_02",
  "metric_03",
  "metric_04",
  "metric_05",
  "metric_06",
  "metric_07",
  "metric_08",
  "metric_09",
  "metric_10",
  "metric_11",
  "metric_12",
  "metric_13",
  "metric_14",
  "metric_15",
  "metric_16",
  "metric_17",
  "metric_18",
  "metric_19",
  "metric_20",
  "counter_01",
  "counter_02",
  "counter_03",
  "counter_04",
  "counter_05",
  "attr_01",
  "attr_02",
  "attr_03",
  "attr_04",
  "attr_05",
  "tag_01",
  "tag_02",
  "tag_03",
  "ext_json"
)

// 根据表号拼出 Iceberg 全表名，如 local.test_db.iot_wide_0001。
def icebergTableName(tableNo: Int): String = {
  f"${icebergNamespace}.${icebergTablePrefix}${tableNo}%04d"
}

// 根据表号拼出 YMatrix 目标表名，如 iot_wide_0001。
def gpTableName(tableNo: Int): String = {
  f"${ymatrixTablePrefix}${tableNo}%04d"
}

// 通过 its-ymatrix connector 执行查询并返回 DataFrame。
def ymatrixRead(query: String): DataFrame = {
  spark.read
    // 使用 its-ymatrix connector 读取。
    .format("its-ymatrix")
    // 指定 JDBC 地址。
    .option("url", ymatrixUrl)
    // 指定用户名。
    .option("user", ymatrixUser)
    // 指定密码。
    .option("password", ymatrixPassword)
    // 把 dbtable 作为 SQL 查询传入。
    .option("dbtable", query)
    // 加载查询结果。
    .load()
}

// 封装一个通用 JDBC 连接函数，便于重复复用。
def withYMatrixConnection[T](f: java.sql.Connection => T): T = {
  // 建立 JDBC 连接。
  val conn = DriverManager.getConnection(ymatrixUrl, ymatrixUser, ymatrixPassword)
  // 执行业务逻辑并确保连接关闭。
  try f(conn)
  finally conn.close()
}

// 为 SQL 标识符加双引号，避免大小写和关键字问题。
def quoteIdent(ident: String): String = {
  "\"" + ident.replace("\"", "\"\"") + "\""
}

// 判断 YMatrix 目标表是否已经存在。
def ymatrixTableExists(tableName: String): Boolean = {
  withYMatrixConnection { conn =>
    // 构造查询 pg_tables 的 SQL。
    val sql =
      """
        |select count(*)::bigint
        |from pg_catalog.pg_tables
        |where schemaname = ?
        |  and tablename = ?
        |""".stripMargin

    // 预编译 SQL。
    val stmt = conn.prepareStatement(sql)
    try {
      // 绑定 schema 参数。
      stmt.setString(1, ymatrixSchema)
      // 绑定表名参数。
      stmt.setString(2, tableName)
      // 执行查询。
      val rs = stmt.executeQuery()
      try {
        // 返回是否存在至少一条匹配记录。
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

// 读取 Iceberg 表当前最大的 ingest_id。
def currentIcebergMaxIngestId(fullTableName: String): Long = {
  spark.table(fullTableName)
    // 聚合得到最大 ingest_id，空表时回填 0。
    .agg(coalesce(max(col("ingest_id")), lit(0L)))
    // 转成 Long Dataset。
    .as[Long]
    // 取第一条结果。
    .head()
}

// 读取 YMatrix 目标表当前最大的 ingest_id。
def currentYMatrixMaxIngestId(tableName: String): Long = {
  // 如果目标表还不存在，则认为当前水位是 0。
  if (!ymatrixTableExists(tableName)) {
    0L
  } else {
    // 如果表存在，则通过 JDBC 查询 max(ingest_id)。
    withYMatrixConnection { conn =>
      // 拼出带 schema 的目标表名。
      val qualifiedTable = s"${quoteIdent(ymatrixSchema)}.${quoteIdent(tableName)}"
      // 构造查询语句。
      val stmt = conn.prepareStatement(s"select coalesce(max(ingest_id), 0)::bigint from ${qualifiedTable}")
      try {
        // 执行查询。
        val rs = stmt.executeQuery()
        try {
          // 返回最大 ingest_id；若无结果则返回 0。
          if (rs.next()) rs.getLong(1) else 0L
        } finally {
          // 关闭结果集。
          rs.close()
        }
      } finally {
        // 关闭语句对象。
        stmt.close()
      }
    }
  }
}

// 构造某个 ingest_id 区间内的一批源数据。
def buildBatch(fullTableName: String, startId: Long, endIdInclusive: Long): DataFrame = {
  spark.table(fullTableName)
    // 只保留当前批次 ingest_id 范围内的数据。
    .where(col("ingest_id").between(lit(startId), lit(endIdInclusive)))
    // 按 wideColumns 指定列顺序输出。
    .select(wideColumns.map(col): _*)
}

// 先对未 repartition 的批次做轻量判空，避免为了空判断触发整轮大 shuffle。
def hasAnyRows(batchDf: DataFrame): Boolean = {
  batchDf.limit(1).take(1).nonEmpty
}

// 只有确认批次非空后，才按目标分布键组织写入分区。
def prepareWriteBatch(batchDf: DataFrame): DataFrame = {
  batchDf.repartition(writePartitions, col(ymatrixDistributedBy))
}

// 把一批 DataFrame 数据追加写入 YMatrix。
def writeBatchToYMatrix(batchDf: DataFrame, tableName: String): Unit = {
  batchDf.write
    // 使用 its-ymatrix connector。
    .format("its-ymatrix")
    // 指定 JDBC 地址。
    .option("url", ymatrixUrl)
    // 指定用户名。
    .option("user", ymatrixUser)
    // 指定密码。
    .option("password", ymatrixPassword)
    // 指定 schema。
    .option("dbschema", ymatrixSchema)
    // 指定目标表。
    .option("dbtable", tableName)
    // 指定分布键。
    .option("distributedby", ymatrixDistributedBy)
    // 指定网络超时。
    .option("network.timeout", ymatrixNetworkTimeout)
    // 指定服务端超时。
    .option("server.timeout", ymatrixServerTimeout)
    // 指定数据库消息级别。
    .option("dbmessages", ymatrixDbMessages)
    // 按 append 方式写入，便于断点续跑。
    .mode("append")
    // 触发写入。
    .save()
}

// 复制指定编号的一张表。
def copyTable(tableNo: Int): Unit = {
  // 计算源表名。
  val sourceTable = icebergTableName(tableNo)
  // 计算目标表名。
  val targetTable = gpTableName(tableNo)

  // 获取源表当前最大 ingest_id。
  val sourceMaxIngestId = currentIcebergMaxIngestId(sourceTable)
  // 如果源表为空，则直接跳过。
  if (sourceMaxIngestId <= 0L) {
    println(s"[skip] ${sourceTable} is empty")
    return
  }

  // 获取目标表当前最大 ingest_id，作为续跑起点判断依据。
  val targetMaxIngestId = currentYMatrixMaxIngestId(targetTable)
  // 如果目标端已经追平或超过源端，则跳过复制。
  if (targetMaxIngestId >= sourceMaxIngestId) {
    println(
      s"[skip] ${sourceTable} already copied to ${ymatrixSchema}.${targetTable}, " +
        s"gpMax=${targetMaxIngestId}, icebergMax=${sourceMaxIngestId}"
    )
    return
  }

  // 计算本次应从哪个 ingest_id 开始继续复制。
  var nextStartId = targetMaxIngestId + 1L

  // 打印当前表的复制计划。
  println(
    s"[table] copying ${sourceTable} -> ${ymatrixSchema}.${targetTable}, " +
      s"resumeFrom=${nextStartId}, sourceMax=${sourceMaxIngestId}"
  )

  // 只要还没追平源端水位，就持续按批次复制。
  while (nextStartId <= sourceMaxIngestId) {
    // 计算当前批次的结束 ingest_id。
    val nextEndId = math.min(nextStartId + rowsPerBatch - 1L, sourceMaxIngestId)
    // 读取这一批的源数据。
    val batchDf = buildBatch(sourceTable, nextStartId, nextEndId)

    if (hasAnyRows(batchDf)) {
      // 只有非空批次才执行 repartition 和写入，减少写前无效 shuffle。
      val writeDf = prepareWriteBatch(batchDf)
      writeBatchToYMatrix(writeDf, targetTable)

      // 打印批次完成日志。
      println(
        s"[batch] ${sourceTable} -> ${ymatrixSchema}.${targetTable}, " +
          s"ingest_id=[${nextStartId}, ${nextEndId}]"
      )
    } else {
      println(
        s"[batch-skip] ${sourceTable} -> ${ymatrixSchema}.${targetTable}, " +
          s"ingest_id=[${nextStartId}, ${nextEndId}], reason=no_rows_in_range"
      )
    }

    // 推进下一批次起点。
    nextStartId = nextEndId + 1L
  }

  // 最后一轮完成后再回查一次目标端最大 ingest_id。
  val finalTargetMax = currentYMatrixMaxIngestId(targetTable)
  // 打印整张表复制完成日志。
  println(
    s"[done] ${sourceTable} copied to ${ymatrixSchema}.${targetTable}, " +
      s"finalGpMax=${finalTargetMax}, icebergMax=${sourceMaxIngestId}"
  )
}

// 打印作业启动配置。
println(
  s"""
     |Iceberg -> YMatrix bulk load started.
     |icebergNamespace=${icebergNamespace}
     |icebergTablePrefix=${icebergTablePrefix}
     |ymatrixSchema=${ymatrixSchema}
     |ymatrixTablePrefix=${ymatrixTablePrefix}
     |tableStartIndex=${tableStartIndex}
     |tableCount=${tableCount}
     |rowsPerBatch=${rowsPerBatch}
     |writePartitions=${writePartitions}
     |""".stripMargin
)

// 依次处理指定范围内的所有表。
(tableStartIndex until tableStartIndex + tableCount).foreach(copyTable)

// 打印批量装载结束日志。
println("Iceberg -> YMatrix bulk load finished.")
