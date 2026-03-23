// 导入 Timestamp，用于记录本次装载启动时间。
import java.sql.Timestamp
// 导入 Instant 和 ZoneOffset，用于构造事件时间与 UTC 时间串。
import java.time.{Instant, ZoneOffset}

// 导入 DataFrame 类型。
import org.apache.spark.sql.DataFrame
// 导入 Spark SQL 常用函数。
import org.apache.spark.sql.functions._

/**
 * 这个示例建议通过 spark-shell 运行，并提前在 Spark 中配置好 Iceberg catalog。
 *
 * 示例：
 *
 * spark-shell \
 *   --master local[8] \
 *   --driver-memory 16g \
 *   --conf spark.sql.shuffle.partitions=8
 *
 * scala> :load examples/iceberg-wide-table-bulk-load.scala
 *
 * 默认参数偏向 smoke test。
 * 如果要跑 500 张表 * 1000 万行的大规模场景，建议逐步放大参数。
 */

// 导入 implicits，供 .as[Long] 使用。
import spark.implicits._

// 定义 Iceberg namespace。
val icebergNamespace = "local.test_db"
// 定义表名前缀。
val tablePrefix = "iot_wide_"

// 定义起始表号。
val tableStartIndex = 1
// 定义要生成的表数量。
val tableCount = 10
// 定义每张表的目标总行数。
val totalRowsPerTable = 10000000L
// 定义单次写入批次大小。
val rowsPerBatch = 500000L
// 定义 spark.range 生成数据时的分区数。
val sparkPartitions = 1

// 定义设备维度基数。
val deviceCardinality = 20000
// 定义租户维度基数。
val tenantCardinality = 20
// 定义站点维度基数。
val siteCardinality = 500
// 定义产线维度基数。
val lineCardinality = 100
// 定义事件时间覆盖天数。
val eventSpanDays = 90

// 校验表数量必须为正数。
require(tableCount > 0, "tableCount must be positive")
// 校验每表总行数必须为正数。
require(totalRowsPerTable > 0, "totalRowsPerTable must be positive")
// 校验批次行数必须为正数。
require(rowsPerBatch > 0, "rowsPerBatch must be positive")
// 校验分区数必须为正数。
require(sparkPartitions > 0, "sparkPartitions must be positive")

// 把事件跨度换算成秒。
val eventSpanSeconds = eventSpanDays.toLong * 24L * 60L * 60L
// 计算一个“过去 90 天”的起始 epoch 秒。
val baseEpochSeconds = Instant.now().minusSeconds(eventSpanSeconds).getEpochSecond
// 记录本次装载启动时间，用于写进 ext_json。
val loadStartedAt = Timestamp.from(Instant.now())

// 根据表号生成完整表名。
def tableName(tableNo: Int): String = {
  f"${icebergNamespace}.${tablePrefix}${tableNo}%04d"
}

// 如果 namespace 不存在，则创建。
def ensureNamespace(): Unit = {
  spark.sql(s"create namespace if not exists ${icebergNamespace}")
}

// 如果表不存在，则创建一张 Iceberg 宽表。
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

// 查询当前表已有的最大 ingest_id。
def currentMaxIngestId(fullTableName: String): Long = {
  spark.table(fullTableName)
    // 空表时回填 0。
    .agg(coalesce(max(col("ingest_id")), lit(0L)))
    // 转成 Long Dataset。
    .as[Long]
    // 取第一行结果。
    .head()
}

// 构造一个批次的宽表模拟数据。
def buildBatch(tableNo: Int, startId: Long, endExclusiveId: Long): DataFrame = {
  // 先构造 region_code 的 CASE 表达式。
  val regionExpr =
    "case cast(id % 6 as int) " +
      "when 0 then 'east' " +
      "when 1 then 'west' " +
      "when 2 then 'north' " +
      "when 3 then 'south' " +
      "when 4 then 'central' " +
      "else 'overseas' end"

  // 构造 event_time 的表达式，让时间均匀分布在最近 eventSpanDays 范围内。
  val eventTsExpr =
    s"cast(from_unixtime(${baseEpochSeconds} + (id % ${eventSpanSeconds})) as timestamp)"

  // 用 spark.range 生成 [startId, endExclusiveId) 范围的模拟数据。
  spark.range(startId, endExclusiveId, 1, sparkPartitions)
    .select(
      // 直接把 range 的 id 当作 ingest_id。
      col("id").cast("bigint").as("ingest_id"),
      // 生成事件 ID。
      concat(lit(f"evt_${tableNo}%04d_"), lpad(col("id").cast("string"), 12, "0")).as("event_id"),
      // 生成设备 ID。
      format_string("dev_%05d", (col("id") % deviceCardinality).cast("int")).as("device_id"),
      // 生成 tenant_id。
      ((col("id") % tenantCardinality) + 1).cast("int").as("tenant_id"),
      // 生成 site_id。
      ((col("id") % siteCardinality) + 1).cast("int").as("site_id"),
      // 生成 line_id。
      format_string("line_%03d", (col("id") % lineCardinality).cast("int")).as("line_id"),
      // 生成 region_code。
      expr(regionExpr).as("region_code"),
      // 生成 event_time。
      expr(eventTsExpr).as("event_time"),
      // 生成 event_date。
      to_date(expr(eventTsExpr)).as("event_date"),
      // 生成 event_minute。
      date_format(expr(eventTsExpr), "yyyy-MM-dd HH:mm").as("event_minute"),
      // 生成状态码。
      (col("id") % 8).cast("int").as("status_code"),
      // 生成告警等级。
      (col("id") % 4).cast("int").as("alarm_level"),
      // 生成指标列 metric_01。
      expr(s"cast(((id % 1000) + ${tableNo}) / 10.0 as double)").as("metric_01"),
      // 生成指标列 metric_02。
      expr(s"cast(((id % 500) + ${tableNo} * 2) / 5.0 as double)").as("metric_02"),
      // 生成指标列 metric_03。
      expr("cast((id % 3600) / 36.0 as double)").as("metric_03"),
      // 生成指标列 metric_04。
      expr("cast((id % 1440) / 14.4 as double)").as("metric_04"),
      // 生成指标列 metric_05。
      expr("cast((id % 10000) / 100.0 as double)").as("metric_05"),
      // 生成指标列 metric_06。
      expr("cast((id % 12000) / 120.0 as double)").as("metric_06"),
      // 生成指标列 metric_07。
      expr("cast((id % 15000) / 150.0 as double)").as("metric_07"),
      // 生成指标列 metric_08。
      expr("cast((id % 18000) / 180.0 as double)").as("metric_08"),
      // 生成指标列 metric_09。
      expr("cast((id % 20000) / 200.0 as double)").as("metric_09"),
      // 生成指标列 metric_10。
      expr("cast((id % 22000) / 220.0 as double)").as("metric_10"),
      // 生成指标列 metric_11。
      expr("cast((id % 24000) / 240.0 as double)").as("metric_11"),
      // 生成指标列 metric_12。
      expr("cast((id % 26000) / 260.0 as double)").as("metric_12"),
      // 生成指标列 metric_13。
      expr("cast((id % 28000) / 280.0 as double)").as("metric_13"),
      // 生成指标列 metric_14。
      expr("cast((id % 30000) / 300.0 as double)").as("metric_14"),
      // 生成指标列 metric_15。
      expr("cast((id % 32000) / 320.0 as double)").as("metric_15"),
      // 生成指标列 metric_16。
      expr("cast((id % 34000) / 340.0 as double)").as("metric_16"),
      // 生成指标列 metric_17。
      expr("cast((id % 36000) / 360.0 as double)").as("metric_17"),
      // 生成指标列 metric_18。
      expr("cast((id % 38000) / 380.0 as double)").as("metric_18"),
      // 生成指标列 metric_19。
      expr("cast((id % 40000) / 400.0 as double)").as("metric_19"),
      // 生成指标列 metric_20。
      expr("cast((id % 42000) / 420.0 as double)").as("metric_20"),
      // 生成 counter_01。
      (col("id") * 3L).cast("bigint").as("counter_01"),
      // 生成 counter_02。
      (col("id") * 5L).cast("bigint").as("counter_02"),
      // 生成 counter_03。
      (col("id") * 7L).cast("bigint").as("counter_03"),
      // 生成 counter_04。
      (col("id") * 11L).cast("bigint").as("counter_04"),
      // 生成 counter_05。
      (col("id") * 13L).cast("bigint").as("counter_05"),
      // 生成 attr_01。
      format_string("model_%02d", (col("id") % 20).cast("int")).as("attr_01"),
      // 生成 attr_02。
      format_string("vendor_%02d", (col("id") % 12).cast("int")).as("attr_02"),
      // 生成 attr_03。
      format_string("shift_%02d", (col("id") % 4).cast("int")).as("attr_03"),
      // 生成 attr_04。
      format_string("firmware_%03d", (col("id") % 100).cast("int")).as("attr_04"),
      // 生成 attr_05。
      format_string("batch_%05d", ((col("id") / 1000) % 10000).cast("int")).as("attr_05"),
      // 生成 tag_01。
      format_string("tag_%02d", (col("id") % 16).cast("int")).as("tag_01"),
      // 生成 tag_02。
      format_string("tag_%02d", (col("id") % 32).cast("int")).as("tag_02"),
      // 生成 tag_03。
      format_string("tag_%02d", (col("id") % 64).cast("int")).as("tag_03"),
      // 构造 ext_json，记录表号、序号、事件时间和装载启动时间。
      to_json(struct(
        lit(tableNo).as("table_no"),
        col("id").as("seq_no"),
        expr(eventTsExpr).as("event_time"),
        lit(loadStartedAt.toInstant.atOffset(ZoneOffset.UTC).toString).as("load_started_at_utc")
      )).as("ext_json")
    )
}

// 把指定表号的数据按批量补齐到目标总行数。
def loadTable(tableNo: Int): Unit = {
  // 计算完整表名。
  val fullTableName = tableName(tableNo)
  // 确保表已创建。
  ensureTable(fullTableName)

  // 读取当前表已经写到的最大 ingest_id。
  val maxIngestId = currentMaxIngestId(fullTableName)
  // 下一批从 maxIngestId + 1 开始。
  var nextStartId = maxIngestId + 1L
  // 目标最后一条 ingest_id 就是 totalRowsPerTable。
  val targetLastId = totalRowsPerTable

  // 如果当前表已经达到目标规模，则直接跳过。
  if (nextStartId > targetLastId) {
    println(s"[skip] ${fullTableName} already has ingest_id up to ${maxIngestId}, target=${targetLastId}")
    return
  }

  // 打印当前表装载计划。
  println(s"[table] loading ${fullTableName}, resumeFrom=${nextStartId}, targetLastId=${targetLastId}")

  // 按批次循环补数，直到达到目标总行数。
  while (nextStartId <= targetLastId) {
    // 计算当前批次右边界（开区间）。
    val nextEndExclusive = math.min(nextStartId + rowsPerBatch, targetLastId + 1L)
    // 计算当前批次行数。
    val batchRowCount = nextEndExclusive - nextStartId
    // 构造当前批次 DataFrame。
    val batchDf = buildBatch(tableNo, nextStartId, nextEndExclusive)

    // 把当前批次追加写入 Iceberg。
    batchDf.writeTo(fullTableName).append()

    // 打印批次完成日志。
    println(
      s"[batch] ${fullTableName} wrote ${batchRowCount} rows, " +
        s"ingest_id=[${nextStartId}, ${nextEndExclusive - 1}]"
    )

    // 推进到下一批起点。
    nextStartId = nextEndExclusive
  }

  // 打印当前表装载完成日志。
  println(s"[done] ${fullTableName} reached target rows=${totalRowsPerTable}")
}

// 确保 namespace 已存在。
ensureNamespace()

// 打印装载启动参数。
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

// 按顺序装载指定范围内的所有表。
(tableStartIndex until tableStartIndex + tableCount).foreach(loadTable)

// 打印全部装载结束日志。
println("Iceberg bulk load finished.")
