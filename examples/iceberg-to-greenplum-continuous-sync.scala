import java.sql.Timestamp
import java.time.Instant

import org.apache.spark.sql.functions._

/**
 * This sample is intended to be run via spark-shell.
 *
 * Start spark-shell with:
 *
 * export SPARK_LOCAL_IP=172.16.100.32
 * spark-shell \
 *   --master local[1] \
 *   --conf spark.driver.host=172.16.100.32 \
 *   --conf spark.driver.bindAddress=0.0.0.0 \
 *   --conf spark.local.ip=172.16.100.32 \
 *   --jars ./spark-greenplum-connector_2.12-3.1.jar
 *
 * Then load this script:
 * scala> :load examples/iceberg-to-greenplum-continuous-sync.scala
 *
 * The script keeps appending synthetic rows into an Iceberg table and then
 * incrementally copies the new rows into Greenplum.
 *
 * Before running the sync loop, create the target table in Greenplum:
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

import spark.implicits._

val icebergNamespace = "local.test_db"
val icebergTable = s"${icebergNamespace}.orders_demo"

val gpUrl = "jdbc:postgresql://172.16.100.29:5432/zhangchen"
val gpUser = "zhangchen"
val gpPassword = "YMatrix@123"
val gpSchema = "public"
val gpTable = "iceberg_sync_demo"

val rowsPerBatch = 10L
val pauseSeconds = 5
val maxPasses = -1 // Set to a positive number if you want a finite demo run.

spark.sql(s"create namespace if not exists ${icebergNamespace}")
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

println(s"Iceberg table ready: ${icebergTable}")
println(s"Greenplum target table: ${gpSchema}.${gpTable}")

def appendIcebergBatch(startOrderId: Long, batchId: Long, batchSize: Long): Long = {
  val batchTs = Timestamp.from(Instant.now())
  val batchDf = spark.range(startOrderId, startOrderId + batchSize, 1, 1)
    .select(
      col("id").cast("bigint").as("order_id"),
      format_string("user_%04d", (col("id") % 1000).cast("int")).as("user_id"),
      expr("cast(((id % 97) + 1) * 3.25 as decimal(18,2))").as("amount"),
      lit(batchTs).cast("timestamp").as("created_at"),
      lit(batchId).cast("bigint").as("batch_id")
    )

  batchDf.writeTo(icebergTable).append()
  val nextOrderId = startOrderId + batchSize
  println(s"[producer] batch=${batchId} appended ${batchSize} rows into ${icebergTable}, order_id < ${nextOrderId}")
  nextOrderId
}

def syncIcebergIncrement(lastSyncedOrderId: Long): Long = {
  val incrDf = spark.table(icebergTable)
    .where(col("order_id") > lit(lastSyncedOrderId))
    .orderBy(col("order_id"))
    .coalesce(1)
    .cache()

  val rowCount = incrDf.count()
  if (rowCount == 0) {
    incrDf.unpersist()
    println(s"[sync] no new rows after order_id=${lastSyncedOrderId}")
    return lastSyncedOrderId
  }

  val newHighWatermark = incrDf.agg(max("order_id")).as[Long].head()

  incrDf.write
    .format("its-greenplum")
    .option("url", gpUrl)
    .option("user", gpUser)
    .option("password", gpPassword)
    .option("dbschema", gpSchema)
    .option("dbtable", gpTable)
    .option("server.port", "43000")
    .option("network.timeout", "20s")
    .option("server.timeout", "20s")
    .option("dbmessages", "WARN")
    .mode("append")
    .save()

  incrDf.unpersist()
  println(s"[sync] copied ${rowCount} rows to ${gpSchema}.${gpTable}, highWatermark=${newHighWatermark}")
  newHighWatermark
}

var nextOrderId = (
  spark.table(icebergTable)
    .agg(coalesce(max(col("order_id")) + lit(1L), lit(1L)))
    .as[Long]
    .head()
)

var lastSyncedOrderId = (
  spark.read
    .format("its-greenplum")
    .option("url", gpUrl)
    .option("user", gpUser)
    .option("password", gpPassword)
    .option("dbtable", s"select coalesce(max(order_id), 0) as max_order_id from ${gpSchema}.${gpTable}")
    .load()
    .as[Long]
    .head()
)

println(s"Initial nextOrderId=${nextOrderId}, lastSyncedOrderId=${lastSyncedOrderId}")

var passNo = 0
while (maxPasses < 0 || passNo < maxPasses) {
  passNo += 1
  nextOrderId = appendIcebergBatch(nextOrderId, passNo, rowsPerBatch)
  lastSyncedOrderId = syncIcebergIncrement(lastSyncedOrderId)
  Thread.sleep(pauseSeconds * 1000L)
}

println("Loop finished.")
