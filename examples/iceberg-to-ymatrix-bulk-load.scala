import java.sql.DriverManager

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
 * This sample is intended to be run via spark-shell after your Iceberg catalog
 * has already been configured in Spark and the connector jar has been added.
 *
 * Example:
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
 * The script copies historical rows from Iceberg into YMatrix table by table.
 * Resume is supported by comparing max(ingest_id) between source and target.
 */

import spark.implicits._

val icebergNamespace = "local.test_db"
val icebergTablePrefix = "iot_wide_"

val ymatrixUrl = "jdbc:postgresql://172.16.100.29:5432/zhangchen"
val ymatrixUser = "zhangchen"
val ymatrixPassword = "YMatrix@123"
val ymatrixSchema = "public"
val ymatrixTablePrefix = "iot_wide_"

val tableStartIndex = 1
val tableCount = 10
val rowsPerBatch = 200000L
val writePartitions = 8

val ymatrixNetworkTimeout = "120s"
val ymatrixServerTimeout = "120s"
val ymatrixDbMessages = "WARN"
val ymatrixDistributedBy = "ingest_id"

require(tableCount > 0, "tableCount must be positive")
require(rowsPerBatch > 0, "rowsPerBatch must be positive")
require(writePartitions > 0, "writePartitions must be positive")

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

def icebergTableName(tableNo: Int): String = {
  f"${icebergNamespace}.${icebergTablePrefix}${tableNo}%04d"
}

def gpTableName(tableNo: Int): String = {
  f"${ymatrixTablePrefix}${tableNo}%04d"
}

def ymatrixRead(query: String): DataFrame = {
  spark.read
    .format("its-ymatrix")
    .option("url", ymatrixUrl)
    .option("user", ymatrixUser)
    .option("password", ymatrixPassword)
    .option("dbtable", query)
    .load()
}

def withYMatrixConnection[T](f: java.sql.Connection => T): T = {
  val conn = DriverManager.getConnection(ymatrixUrl, ymatrixUser, ymatrixPassword)
  try f(conn)
  finally conn.close()
}

def quoteIdent(ident: String): String = {
  "\"" + ident.replace("\"", "\"\"") + "\""
}

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

def currentIcebergMaxIngestId(fullTableName: String): Long = {
  spark.table(fullTableName)
    .agg(coalesce(max(col("ingest_id")), lit(0L)))
    .as[Long]
    .head()
}

def currentYMatrixMaxIngestId(tableName: String): Long = {
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

def buildBatch(fullTableName: String, startId: Long, endIdInclusive: Long): DataFrame = {
  spark.table(fullTableName)
    .where(col("ingest_id").between(lit(startId), lit(endIdInclusive)))
    .select(wideColumns.map(col): _*)
    .repartition(writePartitions)
}

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

def copyTable(tableNo: Int): Unit = {
  val sourceTable = icebergTableName(tableNo)
  val targetTable = gpTableName(tableNo)

  val sourceMaxIngestId = currentIcebergMaxIngestId(sourceTable)
  if (sourceMaxIngestId <= 0L) {
    println(s"[skip] ${sourceTable} is empty")
    return
  }

  val targetMaxIngestId = currentYMatrixMaxIngestId(targetTable)
  if (targetMaxIngestId >= sourceMaxIngestId) {
    println(
      s"[skip] ${sourceTable} already copied to ${ymatrixSchema}.${targetTable}, " +
        s"gpMax=${targetMaxIngestId}, icebergMax=${sourceMaxIngestId}"
    )
    return
  }

  var nextStartId = targetMaxIngestId + 1L

  println(
    s"[table] copying ${sourceTable} -> ${ymatrixSchema}.${targetTable}, " +
      s"resumeFrom=${nextStartId}, sourceMax=${sourceMaxIngestId}"
  )

  while (nextStartId <= sourceMaxIngestId) {
    val nextEndId = math.min(nextStartId + rowsPerBatch - 1L, sourceMaxIngestId)
    val batchDf = buildBatch(sourceTable, nextStartId, nextEndId)

    writeBatchToYMatrix(batchDf, targetTable)

    println(
      s"[batch] ${sourceTable} -> ${ymatrixSchema}.${targetTable}, " +
        s"rows=${nextEndId - nextStartId + 1L}, ingest_id=[${nextStartId}, ${nextEndId}]"
    )

    nextStartId = nextEndId + 1L
  }

  val finalTargetMax = currentYMatrixMaxIngestId(targetTable)
  println(
    s"[done] ${sourceTable} copied to ${ymatrixSchema}.${targetTable}, " +
      s"finalGpMax=${finalTargetMax}, icebergMax=${sourceMaxIngestId}"
  )
}

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

(tableStartIndex until tableStartIndex + tableCount).foreach(copyTable)

println("Iceberg -> YMatrix bulk load finished.")
