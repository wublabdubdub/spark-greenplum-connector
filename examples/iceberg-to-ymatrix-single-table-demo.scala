import org.apache.spark.sql.functions.col

/**
 * Minimal demo: copy one Iceberg table into one YMatrix table.
 *
 * Run with spark-shell after:
 * 1. your Iceberg catalog has already been configured in Spark
 * 2. the connector jar has been added via --jars
 *
 * Example:
 *
 * spark-shell \
 *   --master local[4] \
 *   --driver-memory 8g \
 *   --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
 *   --conf spark.sql.catalog.local.type=hadoop \
 *   --conf spark.sql.catalog.local.warehouse=/tmp/iceberg-warehouse \
 *   --jars ./spark-ymatrix-connector_2.12-3.1.jar
 *
 * scala> :load examples/iceberg-to-ymatrix-single-table-demo.scala
 */

val icebergTable = "local.demo_db.orders"

val ymatrixUrl = "jdbc:postgresql://127.0.0.1:5432/demo"
val ymatrixUser = "ymatrix_user"
val ymatrixPassword = "ymatrix_password"
val ymatrixSchema = "public"
val ymatrixTable = "orders_from_iceberg"

val writeMode = "append"
val writePartitions = 4
val ymatrixDistributedBy = "id"
val ymatrixNetworkTimeout = "120s"
val ymatrixServerTimeout = "120s"
val ymatrixDbMessages = "WARN"

require(Set("append", "overwrite").contains(writeMode), "writeMode must be append or overwrite")
require(writePartitions > 0, "writePartitions must be positive")

val sourceDf = spark.table(icebergTable).repartition(writePartitions)

val sourceCount = sourceDf.count()
println(s"[source] icebergTable=${icebergTable}, rows=${sourceCount}")

if (sourceCount == 0L) {
  println(s"[skip] ${icebergTable} is empty")
} else {
  println(s"[schema] source columns: ${sourceDf.columns.mkString(", ")}")

  sourceDf.write
    .format("its-ymatrix")
    .option("url", ymatrixUrl)
    .option("user", ymatrixUser)
    .option("password", ymatrixPassword)
    .option("dbschema", ymatrixSchema)
    .option("dbtable", ymatrixTable)
    .option("distributedby", ymatrixDistributedBy)
    .option("network.timeout", ymatrixNetworkTimeout)
    .option("server.timeout", ymatrixServerTimeout)
    .option("dbmessages", ymatrixDbMessages)
    .mode(writeMode)
    .save()

  val ymatrixCount = spark.read
    .format("its-ymatrix")
    .option("url", ymatrixUrl)
    .option("user", ymatrixUser)
    .option("password", ymatrixPassword)
    .option("dbtable", s"select count(*)::bigint as row_count from ${ymatrixSchema}.${ymatrixTable}")
    .load()
    .select(col("row_count").cast("long"))
    .head()
    .getLong(0)

  println(s"[done] copied ${sourceCount} rows from ${icebergTable} to ${ymatrixSchema}.${ymatrixTable}")
  println(s"[verify] ymatrix rows=${ymatrixCount}")
}
