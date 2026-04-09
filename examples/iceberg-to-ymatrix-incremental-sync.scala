// 导入 DriverManager，用于直接通过 JDBC 访问 YMatrix。
import java.sql.{DriverManager, Timestamp}
// 导入 Instant，用于记录水位更新时间。
import java.time.Instant

// 导入 DataFrame 类型，供函数签名使用。
import org.apache.spark.sql.DataFrame
// 导入 Spark SQL 常用函数。
import org.apache.spark.sql.functions._

/**
 * 这个示例建议通过 spark-shell 运行。
 *
 * 示例：
 *
 * spark-shell \
 *   --master local[8] \
 *   --driver-memory 16g \
 *   --conf spark.sql.shuffle.partitions=8 \
 *   --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
 *   --conf spark.sql.catalog.local.type=hadoop \
 *   --conf spark.sql.catalog.local.warehouse=/data/iceberg/warehouse/ \
 *   --jars ./spark-ymatrix-connector_2.12-3.1.jar
 *
 * scala> :load examples/iceberg-to-ymatrix-incremental-sync.scala
 *
 * 这个脚本用于第二阶段压测：
 * 1. 从 Iceberg 宽表中读取新增 ingest_id 区间
 * 2. 将增量部分分批追加到 YMatrix
 * 3. 在 YMatrix 中维护一张 watermark 表，记录各目标表的同步进度
 */

// 导入 implicits，供 .as[Long] 使用。
import spark.implicits._

// 定义 Iceberg namespace。
val icebergNamespace = "local.test_db"
// 定义 Iceberg 表名前缀。
val icebergTablePrefix = "iot_wide_"

// 定义 YMatrix JDBC 地址；如已设置环境变量，则优先使用环境变量。
val ymatrixUrl = sys.env.getOrElse("YMATRIX_URL", "jdbc:postgresql://172.16.100.29:5432/zhangchen")
// 定义 YMatrix 用户名。
val ymatrixUser = sys.env.getOrElse("YMATRIX_USER", "zhangchen")
// 定义 YMatrix 密码。
val ymatrixPassword = sys.env.getOrElse("YMATRIX_PASSWORD", "YMatrix@123")
// 定义 YMatrix schema。
val ymatrixSchema = sys.env.getOrElse("YMATRIX_SCHEMA", "public")
// 定义 YMatrix 目标表名前缀。
val ymatrixTablePrefix = "iot_wide_"
// 定义用于记录同步水位的表名。
val ymatrixWatermarkTable = "iceberg_iot_wide_sync_watermark"

// 定义起始表号。
val tableStartIndex = 1
// 定义要处理的表数量。
val tableCount = 10
// 定义每个批次按 ingest_id 划分的行数区间大小。
val rowsPerBatch = 100000L
// 定义 Spark 侧写入分区数。
val writePartitions = 8
// 定义同步轮数；-1 表示持续同步。
val maxSyncPasses = 1
// 定义相邻两轮同步之间的暂停秒数。
val pauseSeconds = 0

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
// 校验同步轮数不能为 0。
require(maxSyncPasses != 0, "maxSyncPasses cannot be zero")
// 校验暂停秒数不能为负数。
require(pauseSeconds >= 0, "pauseSeconds must be >= 0")

// 定义宽表字段顺序，确保读写和对账时列顺序稳定。
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

// 根据表号拼出 Iceberg 全表名。
def icebergTableName(tableNo: Int): String = {
  f"${icebergNamespace}.${icebergTablePrefix}${tableNo}%04d"
}

// 根据表号拼出 YMatrix 目标表名。
def ymatrixTableName(tableNo: Int): String = {
  f"${ymatrixTablePrefix}${tableNo}%04d"
}

// 封装一个通用 JDBC 连接函数，便于重复复用。
def withYMatrixConnection[T](f: java.sql.Connection => T): T = {
  val conn = DriverManager.getConnection(ymatrixUrl, ymatrixUser, ymatrixPassword)
  try f(conn)
  finally conn.close()
}

// 为 SQL 标识符加双引号，避免大小写和关键字问题。
def quoteIdent(ident: String): String = {
  "\"" + ident.replace("\"", "\"\"") + "\""
}

// 获取 watermark 表的全限定名字。
def qualifiedWatermarkTable: String = {
  s"${quoteIdent(ymatrixSchema)}.${quoteIdent(ymatrixWatermarkTable)}"
}

// 确保 YMatrix schema 和 watermark 表都已存在。
def ensureWatermarkInfra(): Unit = {
  withYMatrixConnection { conn =>
    val stmt = conn.createStatement()
    try {
      stmt.execute(s"create schema if not exists ${quoteIdent(ymatrixSchema)}")
      stmt.execute(
        s"""
           |create table if not exists ${qualifiedWatermarkTable} (
           |  table_name text,
           |  last_synced_ingest_id bigint,
           |  source_max_ingest_id bigint,
           |  last_batch_row_count bigint,
           |  updated_at timestamp,
           |  sync_note text
           |)
           |distributed by (table_name)
           |""".stripMargin
      )
    } finally {
      stmt.close()
    }
  }
}

// 判断 YMatrix 目标表是否已经存在。
def ymatrixTableExists(tableName: String): Boolean = {
  withYMatrixConnection { conn =>
    val sql =
      """
        |select count(*)::bigint
        |from pg_catalog.pg_tables
        |where schemaname = ?
        |  and tablename = ?
        |""".stripMargin

    val stmt = conn.prepareStatement(sql)
    try {
      stmt.setString(1, ymatrixSchema)
      stmt.setString(2, tableName)
      val rs = stmt.executeQuery()
      try {
        rs.next() && rs.getLong(1) > 0L
      } finally {
        rs.close()
      }
    } finally {
      stmt.close()
    }
  }
}

// 读取 Iceberg 表当前最大的 ingest_id。
def currentIcebergMaxIngestId(fullTableName: String): Long = {
  spark.table(fullTableName)
    .agg(coalesce(max(col("ingest_id")), lit(0L)))
    .as[Long]
    .head()
}

// 读取 YMatrix 目标表当前最大的 ingest_id。
def currentTargetMaxIngestId(tableName: String): Long = {
  if (!ymatrixTableExists(tableName)) {
    0L
  } else {
    withYMatrixConnection { conn =>
      val qualifiedTable = s"${quoteIdent(ymatrixSchema)}.${quoteIdent(tableName)}"
      val stmt = conn.prepareStatement(s"select coalesce(max(ingest_id), 0)::bigint from ${qualifiedTable}")
      try {
        val rs = stmt.executeQuery()
        try {
          if (rs.next()) rs.getLong(1) else 0L
        } finally {
          rs.close()
        }
      } finally {
        stmt.close()
      }
    }
  }
}

// 读取 watermark 表中记录的上次同步水位。
def currentWatermark(tableName: String): Long = {
  withYMatrixConnection { conn =>
    val stmt = conn.prepareStatement(
      s"select coalesce(max(last_synced_ingest_id), 0)::bigint from ${qualifiedWatermarkTable} where table_name = ?"
    )
    try {
      stmt.setString(1, tableName)
      val rs = stmt.executeQuery()
      try {
        if (rs.next()) rs.getLong(1) else 0L
      } finally {
        rs.close()
      }
    } finally {
      stmt.close()
    }
  }
}

// 综合 watermark 表和目标表当前最大 ingest_id，得出真正的续跑水位。
def currentResumeIngestId(tableName: String): Long = {
  if (!ymatrixTableExists(tableName)) {
    0L
  } else {
    val targetMaxIngestId = currentTargetMaxIngestId(tableName)
    if (targetMaxIngestId <= 0L) 0L else math.max(currentWatermark(tableName), targetMaxIngestId)
  }
}

// 更新指定目标表的同步水位。
def upsertWatermark(
  tableName: String,
  lastSyncedIngestId: Long,
  sourceMaxIngestId: Long,
  lastBatchRowCount: Long,
  syncNote: String
): Unit = {
  withYMatrixConnection { conn =>
    val oldAutoCommit = conn.getAutoCommit
    conn.setAutoCommit(false)

    try {
      val deleteStmt = conn.prepareStatement(s"delete from ${qualifiedWatermarkTable} where table_name = ?")
      try {
        deleteStmt.setString(1, tableName)
        deleteStmt.executeUpdate()
      } finally {
        deleteStmt.close()
      }

      val insertStmt = conn.prepareStatement(
        s"""
           |insert into ${qualifiedWatermarkTable} (
           |  table_name,
           |  last_synced_ingest_id,
           |  source_max_ingest_id,
           |  last_batch_row_count,
           |  updated_at,
           |  sync_note
           |) values (?, ?, ?, ?, ?, ?)
           |""".stripMargin
      )
      try {
        insertStmt.setString(1, tableName)
        insertStmt.setLong(2, lastSyncedIngestId)
        insertStmt.setLong(3, sourceMaxIngestId)
        insertStmt.setLong(4, lastBatchRowCount)
        insertStmt.setTimestamp(5, Timestamp.from(Instant.now()))
        insertStmt.setString(6, syncNote)
        insertStmt.executeUpdate()
      } finally {
        insertStmt.close()
      }

      conn.commit()
    } catch {
      case e: Throwable =>
        conn.rollback()
        throw e
    } finally {
      conn.setAutoCommit(oldAutoCommit)
    }
  }
}

// 构造某个 ingest_id 区间内的一批源数据。
def buildBatch(fullTableName: String, startId: Long, endIdInclusive: Long): DataFrame = {
  spark.table(fullTableName)
    .where(col("ingest_id").between(lit(startId), lit(endIdInclusive)))
    .select(wideColumns.map(col): _*)
}

// 先对过滤后的源批次做轻量判空，避免把空判断放到 repartition 之后。
def hasAnyRows(batchDf: DataFrame): Boolean = {
  batchDf.limit(1).take(1).nonEmpty
}

// 只有确认非空后，才按目标分布键重分区用于写入。
def prepareWriteBatch(batchDf: DataFrame): DataFrame = {
  batchDf.repartition(writePartitions, col(ymatrixDistributedBy))
}

// 把一批 DataFrame 数据追加写入 YMatrix。
def writeBatchToYMatrix(batchDf: DataFrame, tableName: String): Unit = {
  batchDf.write
    .format("its-ymatrix")
    .option("url", ymatrixUrl)
    .option("user", ymatrixUser)
    .option("password", ymatrixPassword)
    .option("dbschema", ymatrixSchema)
    .option("dbtable", tableName)
    .option("distributedby", ymatrixDistributedBy)
    .option("network.timeout", ymatrixNetworkTimeout)
    .option("server.timeout", ymatrixServerTimeout)
    .option("dbmessages", ymatrixDbMessages)
    .mode("append")
    .save()
}

// 同步指定编号的一张表。
def syncTable(tableNo: Int, passNo: Int): Unit = {
  val sourceTable = icebergTableName(tableNo)
  val targetTable = ymatrixTableName(tableNo)

  if (!spark.catalog.tableExists(sourceTable)) {
    println(s"[skip] ${sourceTable} does not exist")
    return
  }

  val sourceMaxIngestId = currentIcebergMaxIngestId(sourceTable)
  if (sourceMaxIngestId <= 0L) {
    println(s"[skip] ${sourceTable} is empty")
    return
  }

  var lastSyncedIngestId = currentResumeIngestId(targetTable)
  if (lastSyncedIngestId >= sourceMaxIngestId) {
    upsertWatermark(
      targetTable,
      lastSyncedIngestId,
      sourceMaxIngestId,
      0L,
      s"pass=${passNo}, status=noop"
    )
    println(
      s"[skip] ${sourceTable} already caught up, " +
        s"target=${ymatrixSchema}.${targetTable}, lastSynced=${lastSyncedIngestId}, sourceMax=${sourceMaxIngestId}"
    )
    return
  }

  println(
    s"[table] syncing ${sourceTable} -> ${ymatrixSchema}.${targetTable}, " +
      s"resumeFrom=${lastSyncedIngestId + 1L}, sourceMax=${sourceMaxIngestId}"
  )

  while (lastSyncedIngestId < sourceMaxIngestId) {
    val nextStartId = lastSyncedIngestId + 1L
    val nextEndId = math.min(nextStartId + rowsPerBatch - 1L, sourceMaxIngestId)
    val batchRowCount = nextEndId - nextStartId + 1L
    val batchDf = buildBatch(sourceTable, nextStartId, nextEndId)

    if (hasAnyRows(batchDf)) {
      val writeDf = prepareWriteBatch(batchDf)
      writeBatchToYMatrix(writeDf, targetTable)

      upsertWatermark(
        targetTable,
        nextEndId,
        sourceMaxIngestId,
        batchRowCount,
        s"pass=${passNo}, ingest_id=[${nextStartId}, ${nextEndId}]"
      )

      println(
        s"[batch] ${sourceTable} -> ${ymatrixSchema}.${targetTable}, " +
          s"rows=${batchRowCount}, ingest_id=[${nextStartId}, ${nextEndId}]"
      )
    } else {
      upsertWatermark(
        targetTable,
        nextEndId,
        sourceMaxIngestId,
        0L,
        s"pass=${passNo}, ingest_id=[${nextStartId}, ${nextEndId}], status=no_rows_in_range"
      )

      println(
        s"[batch-skip] ${sourceTable} -> ${ymatrixSchema}.${targetTable}, " +
          s"ingest_id=[${nextStartId}, ${nextEndId}], reason=no_rows_in_range"
      )
    }

    lastSyncedIngestId = nextEndId
  }

  println(
    s"[done] ${sourceTable} synced to ${ymatrixSchema}.${targetTable}, " +
      s"lastSynced=${lastSyncedIngestId}, sourceMax=${sourceMaxIngestId}"
  )
}

// 确保 watermark 基础设施已经建好。
ensureWatermarkInfra()

// 打印作业启动配置。
println(
  s"""
     |Iceberg -> YMatrix incremental sync started.
     |icebergNamespace=${icebergNamespace}
     |icebergTablePrefix=${icebergTablePrefix}
     |ymatrixSchema=${ymatrixSchema}
     |ymatrixTablePrefix=${ymatrixTablePrefix}
     |ymatrixWatermarkTable=${ymatrixWatermarkTable}
     |tableStartIndex=${tableStartIndex}
     |tableCount=${tableCount}
     |rowsPerBatch=${rowsPerBatch}
     |writePartitions=${writePartitions}
     |maxSyncPasses=${maxSyncPasses}
     |pauseSeconds=${pauseSeconds}
     |""".stripMargin
)

// 初始化同步轮数。
var passNo = 0

// 按配置执行一轮或多轮增量同步。
while (maxSyncPasses < 0 || passNo < maxSyncPasses) {
  passNo += 1
  println(s"[pass] start pass=${passNo}")

  (tableStartIndex until tableStartIndex + tableCount).foreach(tableNo => syncTable(tableNo, passNo))

  println(s"[pass] finished pass=${passNo}")

  if (pauseSeconds > 0 && (maxSyncPasses < 0 || passNo < maxSyncPasses)) {
    Thread.sleep(pauseSeconds * 1000L)
  }
}

// 打印增量同步结束日志。
println("Iceberg -> YMatrix incremental sync finished.")
