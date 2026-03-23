// 导入 Timestamp，用于记录每轮增量写入时间。
import java.sql.Timestamp
// 导入 Instant 和 ZoneOffset，用于构造最近时间窗口内的事件时间。
import java.time.{Instant, ZoneOffset}
// 导入随机数工具，用于控制每张表每轮新增行数。
import scala.util.Random

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
 * scala> :load examples/iceberg-wide-table-random-increment-load.scala
 *
 * 这个脚本用于第二阶段压测：
 * 1. 在已有 iot_wide_* Iceberg 表上继续追加增量数据
 * 2. 每一轮对每张表随机追加一批记录
 * 3. 依据源表当前 max(ingest_id) 自动续写
 */

// 导入 implicits，供 .as[Long] 使用。
import spark.implicits._

// 定义 Iceberg namespace。
val icebergNamespace = "local.test_db"
// 定义表名前缀。
val tablePrefix = "iot_wide_"

// 定义起始表号。
val tableStartIndex = 1
// 定义要处理的表数量。
val tableCount = 10

// 定义每轮每张表最少新增多少行。
val minRowsPerTablePerRound = 10000L
// 定义每轮每张表最多新增多少行。
val maxRowsPerTablePerRound = 50000L
// 定义要执行多少轮增量写入；-1 表示持续写入。
val totalRounds = 5
// 定义相邻两轮之间的暂停秒数。
val pauseSeconds = 0
// 定义 spark.range 造数分区数。
val sparkPartitions = 2
// 定义固定随机种子，便于复现。
val randomSeed = 20260320L

// 定义设备维度基数。
val deviceCardinality = 20000
// 定义租户维度基数。
val tenantCardinality = 20
// 定义站点维度基数。
val siteCardinality = 500
// 定义产线维度基数。
val lineCardinality = 100
// 定义事件时间只覆盖最近多少天。
val recentEventSpanDays = 7

// 校验表数量必须为正数。
require(tableCount > 0, "tableCount must be positive")
// 校验最小批次行数必须为正数。
require(minRowsPerTablePerRound > 0L, "minRowsPerTablePerRound must be positive")
// 校验最大批次行数必须不小于最小值。
require(maxRowsPerTablePerRound >= minRowsPerTablePerRound, "maxRowsPerTablePerRound must be >= minRowsPerTablePerRound")
// 校验轮数不能为 0。
require(totalRounds != 0, "totalRounds cannot be zero")
// 校验暂停秒数不能为负数。
require(pauseSeconds >= 0, "pauseSeconds must be >= 0")
// 校验造数分区数必须为正数。
require(sparkPartitions > 0, "sparkPartitions must be positive")

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
    .agg(coalesce(max(col("ingest_id")), lit(0L)))
    .as[Long]
    .head()
}

// 计算当前轮当前表要追加的随机行数。
def randomRowsThisRound(random: Random): Long = {
  val span = maxRowsPerTablePerRound - minRowsPerTablePerRound + 1L
  minRowsPerTablePerRound + math.floor(random.nextDouble() * span.toDouble).toLong
}

// 构造一个增量批次的宽表模拟数据。
def buildIncrementBatch(tableNo: Int, roundNo: Int, startId: Long, rowCount: Long): DataFrame = {
  // 计算当前批次的右边界开区间。
  val endExclusiveId = startId + rowCount
  // 把最近时间跨度换算成秒。
  val recentEventSpanSeconds = recentEventSpanDays.toLong * 24L * 60L * 60L
  // 以当前时间往前 recentEventSpanDays 天作为起始时间。
  val baseEpochSeconds = Instant.now().minusSeconds(recentEventSpanSeconds).getEpochSecond
  // 记录本轮启动时间。
  val roundStartedAt = Timestamp.from(Instant.now())

  // 构造 region_code 的 CASE 表达式。
  val regionExpr =
    s"case cast((id + ${tableNo} + ${roundNo}) % 6 as int) " +
      "when 0 then 'east' " +
      "when 1 then 'west' " +
      "when 2 then 'north' " +
      "when 3 then 'south' " +
      "when 4 then 'central' " +
      "else 'overseas' end"

  // 构造一个更偏向“最近数据”的事件时间表达式。
  val eventTsExpr =
    s"cast(from_unixtime(${baseEpochSeconds} + ((id * 13 + ${tableNo} * 101 + ${roundNo} * 1009) % ${recentEventSpanSeconds})) as timestamp)"

  // 用 spark.range 生成 [startId, endExclusiveId) 范围的模拟增量数据。
  spark.range(startId, endExclusiveId, 1, sparkPartitions)
    .select(
      col("id").cast("bigint").as("ingest_id"),
      concat(
        lit(f"evt_inc_${tableNo}%04d_r${roundNo}%03d_"),
        lpad(col("id").cast("string"), 12, "0")
      ).as("event_id"),
      format_string("dev_%05d", ((col("id") + lit(roundNo.toLong)) % deviceCardinality).cast("int")).as("device_id"),
      (((col("id") + lit(roundNo.toLong)) % tenantCardinality) + 1).cast("int").as("tenant_id"),
      (((col("id") + lit(tableNo.toLong)) % siteCardinality) + 1).cast("int").as("site_id"),
      format_string("line_%03d", ((col("id") + lit(roundNo.toLong * 3L)) % lineCardinality).cast("int")).as("line_id"),
      expr(regionExpr).as("region_code"),
      expr(eventTsExpr).as("event_time"),
      to_date(expr(eventTsExpr)).as("event_date"),
      date_format(expr(eventTsExpr), "yyyy-MM-dd HH:mm").as("event_minute"),
      ((col("id") + lit(roundNo.toLong)) % 8).cast("int").as("status_code"),
      ((col("id") + lit(tableNo.toLong + roundNo.toLong)) % 4).cast("int").as("alarm_level"),
      expr(s"cast((((id + ${roundNo}) % 1000) + ${tableNo}) / 10.0 as double)").as("metric_01"),
      expr(s"cast((((id + ${roundNo} * 2) % 500) + ${tableNo} * 2) / 5.0 as double)").as("metric_02"),
      expr(s"cast(((id + ${roundNo}) % 3600) / 36.0 as double)").as("metric_03"),
      expr(s"cast(((id + ${roundNo} * 3) % 1440) / 14.4 as double)").as("metric_04"),
      expr(s"cast(((id + ${roundNo} * 5) % 10000) / 100.0 as double)").as("metric_05"),
      expr(s"cast(((id + ${roundNo} * 7) % 12000) / 120.0 as double)").as("metric_06"),
      expr(s"cast(((id + ${roundNo} * 11) % 15000) / 150.0 as double)").as("metric_07"),
      expr(s"cast(((id + ${roundNo} * 13) % 18000) / 180.0 as double)").as("metric_08"),
      expr(s"cast(((id + ${roundNo} * 17) % 20000) / 200.0 as double)").as("metric_09"),
      expr(s"cast(((id + ${roundNo} * 19) % 22000) / 220.0 as double)").as("metric_10"),
      expr(s"cast(((id + ${roundNo} * 23) % 24000) / 240.0 as double)").as("metric_11"),
      expr(s"cast(((id + ${roundNo} * 29) % 26000) / 260.0 as double)").as("metric_12"),
      expr(s"cast(((id + ${roundNo} * 31) % 28000) / 280.0 as double)").as("metric_13"),
      expr(s"cast(((id + ${roundNo} * 37) % 30000) / 300.0 as double)").as("metric_14"),
      expr(s"cast(((id + ${roundNo} * 41) % 32000) / 320.0 as double)").as("metric_15"),
      expr(s"cast(((id + ${roundNo} * 43) % 34000) / 340.0 as double)").as("metric_16"),
      expr(s"cast(((id + ${roundNo} * 47) % 36000) / 360.0 as double)").as("metric_17"),
      expr(s"cast(((id + ${roundNo} * 53) % 38000) / 380.0 as double)").as("metric_18"),
      expr(s"cast(((id + ${roundNo} * 59) % 40000) / 400.0 as double)").as("metric_19"),
      expr(s"cast(((id + ${roundNo} * 61) % 42000) / 420.0 as double)").as("metric_20"),
      (col("id") * 3L + lit(roundNo.toLong)).cast("bigint").as("counter_01"),
      (col("id") * 5L + lit(roundNo.toLong)).cast("bigint").as("counter_02"),
      (col("id") * 7L + lit(roundNo.toLong)).cast("bigint").as("counter_03"),
      (col("id") * 11L + lit(roundNo.toLong)).cast("bigint").as("counter_04"),
      (col("id") * 13L + lit(roundNo.toLong)).cast("bigint").as("counter_05"),
      format_string("model_%02d", ((col("id") + lit(roundNo.toLong)) % 20).cast("int")).as("attr_01"),
      format_string("vendor_%02d", ((col("id") + lit(tableNo.toLong)) % 12).cast("int")).as("attr_02"),
      format_string("shift_%02d", ((col("id") + lit(roundNo.toLong)) % 4).cast("int")).as("attr_03"),
      format_string("firmware_%03d", ((col("id") + lit(roundNo.toLong * 9L)) % 100).cast("int")).as("attr_04"),
      format_string("batch_%05d", (((col("id") / 1000L) + lit(roundNo.toLong)) % 10000).cast("int")).as("attr_05"),
      format_string("tag_%02d", ((col("id") + lit(roundNo.toLong)) % 16).cast("int")).as("tag_01"),
      format_string("tag_%02d", ((col("id") + lit(roundNo.toLong * 2L)) % 32).cast("int")).as("tag_02"),
      format_string("tag_%02d", ((col("id") + lit(roundNo.toLong * 4L)) % 64).cast("int")).as("tag_03"),
      to_json(struct(
        lit("incremental_load").as("write_mode"),
        lit(tableNo).as("table_no"),
        lit(roundNo).as("round_no"),
        col("id").as("seq_no"),
        expr(eventTsExpr).as("event_time"),
        lit(roundStartedAt.toInstant.atOffset(ZoneOffset.UTC).toString).as("round_started_at_utc")
      )).as("ext_json")
    )
}

// 执行一轮“随机增量写入”。
def runOneRound(roundNo: Int, random: Random): Unit = {
  // 打乱表的执行顺序，尽量模拟更真实的多表写入节奏。
  val tableNos = (tableStartIndex until tableStartIndex + tableCount).toList.sortBy(_ => random.nextLong())

  println(s"[round] start round=${roundNo}, tableCount=${tableNos.size}")

  tableNos.foreach { tableNo =>
    val fullTableName = tableName(tableNo)
    ensureTable(fullTableName)

    val nextStartId = currentMaxIngestId(fullTableName) + 1L
    val rowCount = randomRowsThisRound(random)
    val batchDf = buildIncrementBatch(tableNo, roundNo, nextStartId, rowCount)

    batchDf.writeTo(fullTableName).append()

    println(
      s"[batch] round=${roundNo}, table=${fullTableName}, " +
        s"rows=${rowCount}, ingest_id=[${nextStartId}, ${nextStartId + rowCount - 1L}]"
    )
  }

  println(s"[round] finished round=${roundNo}")
}

// 确保 namespace 已存在。
ensureNamespace()

// 打印作业启动参数。
println(
  s"""
     |Iceberg random incremental load started.
     |namespace=${icebergNamespace}
     |tablePrefix=${tablePrefix}
     |tableStartIndex=${tableStartIndex}
     |tableCount=${tableCount}
     |minRowsPerTablePerRound=${minRowsPerTablePerRound}
     |maxRowsPerTablePerRound=${maxRowsPerTablePerRound}
     |totalRounds=${totalRounds}
     |pauseSeconds=${pauseSeconds}
     |sparkPartitions=${sparkPartitions}
     |randomSeed=${randomSeed}
     |""".stripMargin
)

// 使用固定随机种子，保证同样参数下执行过程可复现。
val random = new Random(randomSeed)
// 初始化轮次计数器。
var roundNo = 0

// 按配置执行指定轮数的增量写入。
while (totalRounds < 0 || roundNo < totalRounds) {
  roundNo += 1
  runOneRound(roundNo, random)

  if (pauseSeconds > 0 && (totalRounds < 0 || roundNo < totalRounds)) {
    Thread.sleep(pauseSeconds * 1000L)
  }
}

// 打印全部增量写入结束日志。
println("Iceberg random incremental load finished.")
