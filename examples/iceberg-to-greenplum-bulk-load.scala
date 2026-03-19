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
 *   --jars ./spark-greenplum-connector_2.12-3.1.jar
 *
 * scala> :load examples/iceberg-to-greenplum-bulk-load.scala
 *
 * The script copies historical rows from Iceberg into Greenplum table by table.
 * Resume is supported by comparing max(ingest_id) between source and target.
 */

import spark.implicits._

val icebergNamespace = "local.test_db"
val icebergTablePrefix = "iot_wide_"

val gpUrl = "jdbc:postgresql://172.16.100.29:5432/zhangchen"
val gpUser = "zhangchen"
val gpPassword = "YMatrix@123"
val gpSchema = "public"
val gpTablePrefix = "iot_wide_"

val tableStartIndex = 1
val tableCount = 10
val rowsPerBatch = 200000L
val writePartitions = 8

val gpNetworkTimeout = "120s"
val gpServerTimeout = "120s"
val gpDbMessages = "WARN"
val gpDistributedBy = "(ingest_id)"

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
  f"${gpTablePrefix}${tableNo}%04d"
}

def gpRead(query: String): DataFrame = {
  spark.read
    .format("its-greenplum")
    .option("url", gpUrl)
    .option("user", gpUser)
    .option("password", gpPassword)
    .option("dbtable", query)
    .load()
}

def gpTableExists(tableName: String): Boolean = {
  val existsQuery =
    s"""
       |select count(*)::bigint as table_cnt
       |from pg_catalog.pg_tables
       |where schemaname = '${gpSchema}'
       |  and tablename = '${tableName}'
       |""".stripMargin

  gpRead(existsQuery).as[Long].head() > 0L
}

def currentIcebergMaxIngestId(fullTableName: String): Long = {
  spark.table(fullTableName)
    .agg(coalesce(max(col("ingest_id")), lit(0L)))
    .as[Long]
    .head()
}

def currentGreenplumMaxIngestId(tableName: String): Long = {
  if (!gpTableExists(tableName)) {
    0L
  } else {
    gpRead(s"select coalesce(max(ingest_id), 0)::bigint as max_ingest_id from ${gpSchema}.${tableName}")
      .as[Long]
      .head()
  }
}

def buildBatch(fullTableName: String, startId: Long, endIdInclusive: Long): DataFrame = {
  spark.table(fullTableName)
    .where(col("ingest_id").between(lit(startId), lit(endIdInclusive)))
    .select(wideColumns.map(col): _*)
    .repartition(writePartitions)
}

def writeBatchToGreenplum(batchDf: DataFrame, tableName: String): Unit = {
  batchDf.write
    .format("its-greenplum")
    .option("url", gpUrl)
    .option("user", gpUser)
    .option("password", gpPassword)
    .option("dbschema", gpSchema)
    .option("dbtable", tableName)
    .option("distributedby", gpDistributedBy)
    .option("network.timeout", gpNetworkTimeout)
    .option("server.timeout", gpServerTimeout)
    .option("dbmessages", gpDbMessages)
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

  val targetMaxIngestId = currentGreenplumMaxIngestId(targetTable)
  if (targetMaxIngestId >= sourceMaxIngestId) {
    println(
      s"[skip] ${sourceTable} already copied to ${gpSchema}.${targetTable}, " +
        s"gpMax=${targetMaxIngestId}, icebergMax=${sourceMaxIngestId}"
    )
    return
  }

  var nextStartId = targetMaxIngestId + 1L

  println(
    s"[table] copying ${sourceTable} -> ${gpSchema}.${targetTable}, " +
      s"resumeFrom=${nextStartId}, sourceMax=${sourceMaxIngestId}"
  )

  while (nextStartId <= sourceMaxIngestId) {
    val nextEndId = math.min(nextStartId + rowsPerBatch - 1L, sourceMaxIngestId)
    val batchDf = buildBatch(sourceTable, nextStartId, nextEndId)

    writeBatchToGreenplum(batchDf, targetTable)

    println(
      s"[batch] ${sourceTable} -> ${gpSchema}.${targetTable}, " +
        s"rows=${nextEndId - nextStartId + 1L}, ingest_id=[${nextStartId}, ${nextEndId}]"
    )

    nextStartId = nextEndId + 1L
  }

  val finalTargetMax = currentGreenplumMaxIngestId(targetTable)
  println(
    s"[done] ${sourceTable} copied to ${gpSchema}.${targetTable}, " +
      s"finalGpMax=${finalTargetMax}, icebergMax=${sourceMaxIngestId}"
  )
}

println(
  s"""
     |Iceberg -> Greenplum bulk load started.
     |icebergNamespace=${icebergNamespace}
     |icebergTablePrefix=${icebergTablePrefix}
     |gpSchema=${gpSchema}
     |gpTablePrefix=${gpTablePrefix}
     |tableStartIndex=${tableStartIndex}
     |tableCount=${tableCount}
     |rowsPerBatch=${rowsPerBatch}
     |writePartitions=${writePartitions}
     |""".stripMargin
)

(tableStartIndex until tableStartIndex + tableCount).foreach(copyTable)

println("Iceberg -> Greenplum bulk load finished.")
