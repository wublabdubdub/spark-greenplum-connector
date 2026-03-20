import java.sql.DriverManager

import org.apache.spark.sql.functions._

import scala.util.control.NonFatal

val icebergTable = "local.test_db.iot_wide_0001"

val gpUrl = "jdbc:postgresql://172.16.100.29:5432/zhangchen"
val gpUser = "zhangchen"
val gpPassword = "YMatrix@123"
val gpSchema = "public"
val gpTable = "iot_wide_0001"

val writePartitions = 1
val shufflePartitions = 1
val distributedBy = "ingest_id"
val networkTimeout = "300s"
val serverTimeout = "300s"
val dbMessages = "WARN"

case class TableProfile(rowCount: Long, minIngestId: Long, maxIngestId: Long)

def quoteIdent(ident: String): String = "\"" + ident.replace("\"", "\"\"") + "\""

def queryGpProfile(): TableProfile = {
  val sql =
    s"""
       |select
       |  count(*)::bigint as row_count,
       |  coalesce(min(ingest_id), 0)::bigint as min_ingest_id,
       |  coalesce(max(ingest_id), 0)::bigint as max_ingest_id
       |from ${quoteIdent(gpSchema)}.${quoteIdent(gpTable)}
       |""".stripMargin

  val conn = DriverManager.getConnection(gpUrl, gpUser, gpPassword)
  try {
    val stmt = conn.prepareStatement(sql)
    try {
      val rs = stmt.executeQuery()
      try {
        rs.next()
        TableProfile(rs.getLong("row_count"), rs.getLong("min_ingest_id"), rs.getLong("max_ingest_id"))
      } finally {
        rs.close()
      }
    } finally {
      stmt.close()
    }
  } finally {
    conn.close()
  }
}

def probeGp(): Unit = {
  val conn = DriverManager.getConnection(gpUrl, gpUser, gpPassword)
  try {
    val stmt = conn.prepareStatement("select 1")
    try {
      val rs = stmt.executeQuery()
      try {
        require(rs.next() && rs.getInt(1) == 1, "Greenplum JDBC probe failed")
      } finally {
        rs.close()
      }
    } finally {
      stmt.close()
    }
  } finally {
    conn.close()
  }
}

def profileIcebergTable(): TableProfile = {
  val metrics = spark.table(icebergTable)
    .agg(
      count(lit(1)).cast("long").as("row_count"),
      coalesce(min(col("ingest_id")), lit(0L)).cast("long").as("min_ingest_id"),
      coalesce(max(col("ingest_id")), lit(0L)).cast("long").as("max_ingest_id")
    )
    .head()

  TableProfile(
    metrics.getAs[Long]("row_count"),
    metrics.getAs[Long]("min_ingest_id"),
    metrics.getAs[Long]("max_ingest_id")
  )
}

spark.conf.set("spark.sql.shuffle.partitions", shufflePartitions.toString)

val exitCode = try {
  println(
    s"""
       |[config] icebergTable=${icebergTable}
       |[config] gpTarget=${gpSchema}.${gpTable}
       |[config] writePartitions=${writePartitions}
       |[config] shufflePartitions=${shufflePartitions}
       |[config] distributedBy=${distributedBy}
       |""".stripMargin
  )

  val sourceDf = spark.table(icebergTable)
  println(s"[schema] ${sourceDf.schema.treeString}")

  val sourceProfile = profileIcebergTable()
  println(
    s"[source] rowCount=${sourceProfile.rowCount}, " +
      s"minIngestId=${sourceProfile.minIngestId}, maxIngestId=${sourceProfile.maxIngestId}"
  )

  require(sourceProfile.rowCount > 0L, s"${icebergTable} is empty")

  probeGp()
  println("[probe] Greenplum JDBC probe passed")

  sourceDf
    .repartition(writePartitions, col("ingest_id"))
    .write
    .format("its-ymatrix")
    .option("url", gpUrl)
    .option("user", gpUser)
    .option("password", gpPassword)
    .option("dbschema", gpSchema)
    .option("dbtable", gpTable)
    .option("distributedby", distributedBy)
    .option("network.timeout", networkTimeout)
    .option("server.timeout", serverTimeout)
    .option("dbmessages", dbMessages)
    .mode("overwrite")
    .save()

  val targetProfile = queryGpProfile()
  println(
    s"[target] rowCount=${targetProfile.rowCount}, " +
      s"minIngestId=${targetProfile.minIngestId}, maxIngestId=${targetProfile.maxIngestId}"
  )

  require(
    targetProfile.rowCount == sourceProfile.rowCount,
    s"Row count mismatch: source=${sourceProfile.rowCount}, target=${targetProfile.rowCount}"
  )
  require(
    targetProfile.minIngestId == sourceProfile.minIngestId &&
      targetProfile.maxIngestId == sourceProfile.maxIngestId,
    s"Ingest range mismatch: source=[${sourceProfile.minIngestId}, ${sourceProfile.maxIngestId}], " +
      s"target=[${targetProfile.minIngestId}, ${targetProfile.maxIngestId}]"
  )

  println(
    s"[done] Successfully migrated ${sourceProfile.rowCount} rows from ${icebergTable} " +
      s"to ${gpSchema}.${gpTable}"
  )
  0
} catch {
  case NonFatal(e) =>
    e.printStackTrace()
    1
} finally {
  spark.stop()
}

System.exit(exitCode)
