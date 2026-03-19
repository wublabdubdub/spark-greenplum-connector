import java.sql.Timestamp
import java.time.{Instant, ZoneOffset}

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
 * This sample is intended to be run via spark-shell after your Iceberg catalog
 * has already been configured in Spark.
 *
 * Example:
 *
 * spark-shell \
 *   --master local[8] \
 *   --driver-memory 16g \
 *   --conf spark.sql.shuffle.partitions=8
 *
 * scala> :load examples/iceberg-wide-table-bulk-load.scala
 *
 * Default parameters target a small smoke test. Increase them gradually before
 * running the full 500 tables * 10,000,000 rows workload on a single-node Spark.
 */

import spark.implicits._

val icebergNamespace = "local.test_db"
val tablePrefix = "iot_wide_"

val tableStartIndex = 1
val tableCount = 10
val totalRowsPerTable = 1000000L
val rowsPerBatch = 500000L
val sparkPartitions = 8

val deviceCardinality = 20000
val tenantCardinality = 20
val siteCardinality = 500
val lineCardinality = 100
val eventSpanDays = 90

require(tableCount > 0, "tableCount must be positive")
require(totalRowsPerTable > 0, "totalRowsPerTable must be positive")
require(rowsPerBatch > 0, "rowsPerBatch must be positive")
require(sparkPartitions > 0, "sparkPartitions must be positive")

val eventSpanSeconds = eventSpanDays.toLong * 24L * 60L * 60L
val baseEpochSeconds = Instant.now().minusSeconds(eventSpanSeconds).getEpochSecond
val loadStartedAt = Timestamp.from(Instant.now())

def tableName(tableNo: Int): String = {
  f"${icebergNamespace}.${tablePrefix}${tableNo}%04d"
}

def ensureNamespace(): Unit = {
  spark.sql(s"create namespace if not exists ${icebergNamespace}")
}

def ensureTable(fullTableName: String): Unit = {
  spark.sql(
    s"""
       |create table if not exists ${fullTableName} (
       |  ingest_id bigint,
       |  event_id string,
       |  device_id string,
       |  tenant_id int,
       |  site_id int,
       |  line_id string,
       |  region_code string,
       |  event_time timestamp,
       |  event_date date,
       |  event_minute string,
       |  status_code int,
       |  alarm_level int,
       |  metric_01 double,
       |  metric_02 double,
       |  metric_03 double,
       |  metric_04 double,
       |  metric_05 double,
       |  metric_06 double,
       |  metric_07 double,
       |  metric_08 double,
       |  metric_09 double,
       |  metric_10 double,
       |  metric_11 double,
       |  metric_12 double,
       |  metric_13 double,
       |  metric_14 double,
       |  metric_15 double,
       |  metric_16 double,
       |  metric_17 double,
       |  metric_18 double,
       |  metric_19 double,
       |  metric_20 double,
       |  counter_01 bigint,
       |  counter_02 bigint,
       |  counter_03 bigint,
       |  counter_04 bigint,
       |  counter_05 bigint,
       |  attr_01 string,
       |  attr_02 string,
       |  attr_03 string,
       |  attr_04 string,
       |  attr_05 string,
       |  tag_01 string,
       |  tag_02 string,
       |  tag_03 string,
       |  ext_json string
       |) using iceberg
       |partitioned by (days(event_time))
       |""".stripMargin
  )
}

def currentMaxIngestId(fullTableName: String): Long = {
  spark.table(fullTableName)
    .agg(coalesce(max(col("ingest_id")), lit(0L)))
    .as[Long]
    .head()
}

def buildBatch(tableNo: Int, startId: Long, endExclusiveId: Long): DataFrame = {
  val regionExpr =
    "case cast(id % 6 as int) " +
      "when 0 then 'east' " +
      "when 1 then 'west' " +
      "when 2 then 'north' " +
      "when 3 then 'south' " +
      "when 4 then 'central' " +
      "else 'overseas' end"

  val eventTsExpr =
    s"cast(from_unixtime(${baseEpochSeconds} + (id % ${eventSpanSeconds})) as timestamp)"

  spark.range(startId, endExclusiveId, 1, sparkPartitions)
    .select(
      col("id").cast("bigint").as("ingest_id"),
      concat(lit(f"evt_${tableNo}%04d_"), lpad(col("id").cast("string"), 12, "0")).as("event_id"),
      format_string("dev_%05d", (col("id") % deviceCardinality).cast("int")).as("device_id"),
      ((col("id") % tenantCardinality) + 1).cast("int").as("tenant_id"),
      ((col("id") % siteCardinality) + 1).cast("int").as("site_id"),
      format_string("line_%03d", (col("id") % lineCardinality).cast("int")).as("line_id"),
      expr(regionExpr).as("region_code"),
      expr(eventTsExpr).as("event_time"),
      to_date(expr(eventTsExpr)).as("event_date"),
      date_format(expr(eventTsExpr), "yyyy-MM-dd HH:mm").as("event_minute"),
      (col("id") % 8).cast("int").as("status_code"),
      (col("id") % 4).cast("int").as("alarm_level"),
      expr(s"cast(((id % 1000) + ${tableNo}) / 10.0 as double)").as("metric_01"),
      expr(s"cast(((id % 500) + ${tableNo} * 2) / 5.0 as double)").as("metric_02"),
      expr("cast((id % 3600) / 36.0 as double)").as("metric_03"),
      expr("cast((id % 1440) / 14.4 as double)").as("metric_04"),
      expr("cast((id % 10000) / 100.0 as double)").as("metric_05"),
      expr("cast((id % 12000) / 120.0 as double)").as("metric_06"),
      expr("cast((id % 15000) / 150.0 as double)").as("metric_07"),
      expr("cast((id % 18000) / 180.0 as double)").as("metric_08"),
      expr("cast((id % 20000) / 200.0 as double)").as("metric_09"),
      expr("cast((id % 22000) / 220.0 as double)").as("metric_10"),
      expr("cast((id % 24000) / 240.0 as double)").as("metric_11"),
      expr("cast((id % 26000) / 260.0 as double)").as("metric_12"),
      expr("cast((id % 28000) / 280.0 as double)").as("metric_13"),
      expr("cast((id % 30000) / 300.0 as double)").as("metric_14"),
      expr("cast((id % 32000) / 320.0 as double)").as("metric_15"),
      expr("cast((id % 34000) / 340.0 as double)").as("metric_16"),
      expr("cast((id % 36000) / 360.0 as double)").as("metric_17"),
      expr("cast((id % 38000) / 380.0 as double)").as("metric_18"),
      expr("cast((id % 40000) / 400.0 as double)").as("metric_19"),
      expr("cast((id % 42000) / 420.0 as double)").as("metric_20"),
      (col("id") * 3L).cast("bigint").as("counter_01"),
      (col("id") * 5L).cast("bigint").as("counter_02"),
      (col("id") * 7L).cast("bigint").as("counter_03"),
      (col("id") * 11L).cast("bigint").as("counter_04"),
      (col("id") * 13L).cast("bigint").as("counter_05"),
      format_string("model_%02d", (col("id") % 20).cast("int")).as("attr_01"),
      format_string("vendor_%02d", (col("id") % 12).cast("int")).as("attr_02"),
      format_string("shift_%02d", (col("id") % 4).cast("int")).as("attr_03"),
      format_string("firmware_%03d", (col("id") % 100).cast("int")).as("attr_04"),
      format_string("batch_%05d", ((col("id") / 1000) % 10000).cast("int")).as("attr_05"),
      format_string("tag_%02d", (col("id") % 16).cast("int")).as("tag_01"),
      format_string("tag_%02d", (col("id") % 32).cast("int")).as("tag_02"),
      format_string("tag_%02d", (col("id") % 64).cast("int")).as("tag_03"),
      to_json(struct(
        lit(tableNo).as("table_no"),
        col("id").as("seq_no"),
        expr(eventTsExpr).as("event_time"),
        lit(loadStartedAt.toInstant.atOffset(ZoneOffset.UTC).toString).as("load_started_at_utc")
      )).as("ext_json")
    )
}

def loadTable(tableNo: Int): Unit = {
  val fullTableName = tableName(tableNo)
  ensureTable(fullTableName)

  val maxIngestId = currentMaxIngestId(fullTableName)
  var nextStartId = maxIngestId + 1L
  val targetLastId = totalRowsPerTable

  if (nextStartId > targetLastId) {
    println(s"[skip] ${fullTableName} already has ingest_id up to ${maxIngestId}, target=${targetLastId}")
    return
  }

  println(s"[table] loading ${fullTableName}, resumeFrom=${nextStartId}, targetLastId=${targetLastId}")

  while (nextStartId <= targetLastId) {
    val nextEndExclusive = math.min(nextStartId + rowsPerBatch, targetLastId + 1L)
    val batchRowCount = nextEndExclusive - nextStartId
    val batchDf = buildBatch(tableNo, nextStartId, nextEndExclusive)

    batchDf.writeTo(fullTableName).append()

    println(
      s"[batch] ${fullTableName} wrote ${batchRowCount} rows, " +
        s"ingest_id=[${nextStartId}, ${nextEndExclusive - 1}]"
    )

    nextStartId = nextEndExclusive
  }

  println(s"[done] ${fullTableName} reached target rows=${totalRowsPerTable}")
}

ensureNamespace()

println(
  s"""
     |Iceberg bulk load started.
     |namespace=${icebergNamespace}
     |tablePrefix=${tablePrefix}
     |tableStartIndex=${tableStartIndex}
     |tableCount=${tableCount}
     |totalRowsPerTable=${totalRowsPerTable}
     |rowsPerBatch=${rowsPerBatch}
     |sparkPartitions=${sparkPartitions}
     |""".stripMargin
)

(tableStartIndex until tableStartIndex + tableCount).foreach(loadTable)

println("Iceberg bulk load finished.")
