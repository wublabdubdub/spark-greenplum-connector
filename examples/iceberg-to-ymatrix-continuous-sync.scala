// 导入时间戳类型，用于构造 created_at 字段。
import java.sql.Timestamp
// 导入 Instant，用于获取当前时间。
import java.time.Instant

// 导入 Spark SQL 常用函数。
import org.apache.spark.sql.functions._

/**
 * 这个示例用于在 spark-shell 中演示一个“持续增量同步”流程：
 * 1. 先不断向 Iceberg 表追加模拟订单数据
 * 2. 再把新增部分增量同步到 YMatrix
 *
 * 启动 spark-shell 示例：
 *
 * export SPARK_LOCAL_IP=172.16.100.32
 * spark-shell \
 *   --master local[1] \
 *   --conf spark.driver.host=172.16.100.32 \
 *   --conf spark.driver.bindAddress=0.0.0.0 \
 *   --conf spark.local.ip=172.16.100.32 \
 *   --jars ./spark-ymatrix-connector_2.12-3.1.jar
 *
 * 然后执行：
 * scala> :load examples/iceberg-to-ymatrix-continuous-sync.scala
 *
 * 在运行同步循环前，请先在 YMatrix 里建好目标表：
 *
 * create table public.iceberg_sync_demo (
 *   order_id bigint,
 *   user_id text,
 *   amount decimal(18,2),
 *   created_at timestamp,
 *   batch_id bigint
 * )
 * distributed by (order_id);
 */

// 导入 Dataset 编码器转换能力，供 .as[Long] 使用。
import spark.implicits._

// 定义 Iceberg namespace。
val icebergNamespace = "local.test_db"
// 定义 Iceberg 源表全名。
val icebergTable = s"${icebergNamespace}.orders_demo"

// 定义 YMatrix JDBC 地址。
val ymatrixUrl = "jdbc:postgresql://172.16.100.29:5432/zhangchen"
// 定义 YMatrix 用户名。
val ymatrixUser = "zhangchen"
// 定义 YMatrix 密码。
val ymatrixPassword = "YMatrix@123"
// 定义 YMatrix schema。
val ymatrixSchema = "public"
// 定义 YMatrix 目标表名。
val ymatrixTable = "iceberg_sync_demo"

// 定义每轮生成和同步的行数。
val rowsPerBatch = 10L
// 定义每轮循环后的暂停秒数。
val pauseSeconds = 5
// 定义最大循环次数；-1 表示无限循环演示。
val maxPasses = -1 // Set to a positive number if you want a finite demo run.

// 如果 namespace 不存在，则先创建。
spark.sql(s"create namespace if not exists ${icebergNamespace}")
// 如果 Iceberg 表不存在，则创建一张演示用表。
spark.sql(
  s"""
     |create table if not exists ${icebergTable} (
     |  order_id bigint,
     |  user_id string,
     |  amount decimal(18,2),
     |  created_at timestamp,
     |  batch_id bigint
     |) using iceberg
     |""".stripMargin
)

// 打印 Iceberg 表就绪信息。
println(s"Iceberg table ready: ${icebergTable}")
// 打印 YMatrix 目标表信息。
println(s"YMatrix target table: ${ymatrixSchema}.${ymatrixTable}")

// 从单行 DataFrame 中安全读取指定列的 long 值。
def firstLongValue(df: org.apache.spark.sql.DataFrame, columnName: String): Long = {
  // 先取出目标列并显式转成 long。
  val row = df.select(col(columnName).cast("long")).head()
  // 如果没有值，则返回 0；否则返回实际值。
  if (row == null || row.isNullAt(0)) 0L else row.getLong(0)
}

// 生成一批模拟订单数据并追加到 Iceberg。
def appendIcebergBatch(startOrderId: Long, batchId: Long, batchSize: Long): Long = {
  // 记录这一批次的统一生成时间。
  val batchTs = Timestamp.from(Instant.now())
  // 用 spark.range 生成一段连续 order_id，并构造业务列。
  val batchDf = spark.range(startOrderId, startOrderId + batchSize, 1, 1)
    .select(
      // 把 id 列映射成 order_id。
      col("id").cast("bigint").as("order_id"),
      // 生成 user_id。
      format_string("user_%04d", (col("id") % 1000).cast("int")).as("user_id"),
      // 构造一个可重复的金额字段。
      expr("cast(((id % 97) + 1) * 3.25 as decimal(18,2))").as("amount"),
      // 统一填入本批创建时间。
      lit(batchTs).cast("timestamp").as("created_at"),
      // 填入批次号。
      lit(batchId).cast("bigint").as("batch_id")
    )

  // 将当前批次追加写入 Iceberg 表。
  batchDf.writeTo(icebergTable).append()
  // 计算下一批次的起始 order_id。
  val nextOrderId = startOrderId + batchSize
  // 打印生产侧日志。
  println(s"[producer] batch=${batchId} appended ${batchSize} rows into ${icebergTable}, order_id < ${nextOrderId}")
  // 返回下一批起始 order_id。
  nextOrderId
}

// 从 Iceberg 中读取尚未同步的增量数据并写入 YMatrix。
def syncIcebergIncrement(lastSyncedOrderId: Long): Long = {
  // 读取 order_id 大于上次水位的增量数据。
  val incrDf = spark.table(icebergTable)
    // 只保留未同步的新数据。
    .where(col("order_id") > lit(lastSyncedOrderId))
    // 按 order_id 排序，便于观察和稳定推进水位。
    .orderBy(col("order_id"))
    // 压成一个分区，方便小批量演示。
    .coalesce(1)
    // 缓存起来，避免 count / max / write 重复计算。
    .cache()

  // 统计当前增量行数。
  val rowCount = incrDf.count()
  // 如果没有新增数据，则释放缓存并直接返回旧水位。
  if (rowCount == 0) {
    // 释放缓存。
    incrDf.unpersist()
    // 打印无增量日志。
    println(s"[sync] no new rows after order_id=${lastSyncedOrderId}")
    // 返回上次同步水位。
    return lastSyncedOrderId
  }

  // 计算这批增量数据的最新 order_id，作为新的高水位。
  val newHighWatermark = incrDf.agg(max("order_id")).as[Long].head()

  // 把增量数据追加写入 YMatrix。
  incrDf.write
    // 指定 connector。
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
    .option("dbtable", ymatrixTable)
    // 指定固定 gpfdist 服务端口，便于调试。
    .option("server.port", "43000")
    // 指定较短的网络超时。
    .option("network.timeout", "20s")
    // 指定较短的服务端超时。
    .option("server.timeout", "20s")
    // 指定数据库消息级别。
    .option("dbmessages", "WARN")
    // 使用 append 模式追加。
    .mode("append")
    // 触发写入。
    .save()

  // 写入完成后释放缓存。
  incrDf.unpersist()
  // 打印同步完成日志。
  println(s"[sync] copied ${rowCount} rows to ${ymatrixSchema}.${ymatrixTable}, highWatermark=${newHighWatermark}")
  // 返回新的同步水位。
  newHighWatermark
}

// 计算 Iceberg 侧下一条待生成记录的 order_id 起点。
var nextOrderId = (
  spark.table(icebergTable)
    // 如果表里已有数据，则从 max(order_id)+1 开始；否则从 1 开始。
    .agg(coalesce(max(col("order_id")) + lit(1L), lit(1L)))
    // 转成 Long Dataset。
    .as[Long]
    // 取第一条结果。
    .head()
)

// 从 YMatrix 回查当前已同步到的最高 order_id。
var lastSyncedOrderId = (
  firstLongValue(
    spark.read
      // 通过 connector 读取 YMatrix 查询结果。
      .format("its-ymatrix")
      // 指定 JDBC 地址。
      .option("url", ymatrixUrl)
      // 指定用户名。
      .option("user", ymatrixUser)
      // 指定密码。
      .option("password", ymatrixPassword)
      // 查询 YMatrix 当前最大 order_id。
      .option("dbtable", s"select coalesce(max(order_id), 0) as max_order_id from ${ymatrixSchema}.${ymatrixTable}")
      // 加载结果。
      .load(),
    // 指定要读取的列名。
    "max_order_id"
  )
)

// 打印初始状态。
println(s"Initial nextOrderId=${nextOrderId}, lastSyncedOrderId=${lastSyncedOrderId}")

// 初始化循环次数。
var passNo = 0
// 按配置持续执行“生产 + 同步”循环。
while (maxPasses < 0 || passNo < maxPasses) {
  // 当前轮次加 1。
  passNo += 1
  // 先向 Iceberg 追加一批新数据。
  nextOrderId = appendIcebergBatch(nextOrderId, passNo, rowsPerBatch)
  // 再把未同步部分增量写入 YMatrix。
  lastSyncedOrderId = syncIcebergIncrement(lastSyncedOrderId)
  // 暂停一段时间，模拟周期性同步。
  Thread.sleep(pauseSeconds * 1000L)
}

// 打印循环结束日志。
println("Loop finished.")
