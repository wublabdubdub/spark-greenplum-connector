package com.xiaomi

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

import java.net.InetAddress
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

object LocalSparkIcebergYMatrixTimingApp {
  private val logger: Logger = Logger.getLogger(this.getClass)

  private def resolveLocalIp(): String = {
    sys.env.get("SPARK_LOCAL_IP")
      .filter(_.trim.nonEmpty)
      .getOrElse(InetAddress.getLocalHost.getHostAddress)
  }

  def main(args: Array[String]): Unit = {
    val localIp = resolveLocalIp()
    val warehouse = sys.env.getOrElse("ICEBERG_WAREHOUSE", "/tmp/iceberg_warehouse_local_test")
    val ymatrixUrl = sys.env.getOrElse("YMATRIX_URL", "jdbc:postgresql://172.16.100.62:5432/zhangchen")
    val ymatrixUser = sys.env.getOrElse("YMATRIX_USER", "admin")
    val ymatrixPassword = sys.env.getOrElse("YMATRIX_PASSWORD", "Ymatrix@123")
    val ymatrixSchema = sys.env.getOrElse("YMATRIX_SCHEMA", "public")
    val ymatrixNetworkTimeout = sys.env.getOrElse("YMATRIX_NETWORK_TIMEOUT", "300s")
    val ymatrixServerTimeout = sys.env.getOrElse("YMATRIX_SERVER_TIMEOUT", "300s")
    val ymatrixDbMessages = sys.env.getOrElse("YMATRIX_DBMESSAGES", "OFF")
    val targetTable = sys.env.getOrElse(
      "YMATRIX_TARGET_TABLE",
      s"iceberg_yamtrix_test_${LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"))}"
    )

    logger.warn(s"[config] localIp=$localIp")
    logger.warn(s"[config] warehouse=$warehouse")
    logger.warn(s"[config] ymatrixUrl=$ymatrixUrl")
    logger.warn(s"[config] ymatrixUser=$ymatrixUser")
    logger.warn(s"[config] ymatrixNetworkTimeout=$ymatrixNetworkTimeout")
    logger.warn(s"[config] ymatrixServerTimeout=$ymatrixServerTimeout")
    logger.warn(s"[config] ymatrixDbMessages=$ymatrixDbMessages")
    logger.warn(s"[config] targetTable=$ymatrixSchema.$targetTable")

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("IcebergLocalYMatrixTimingTest")
      .config("spark.driver.host", localIp)
      .config("spark.driver.bindAddress", "0.0.0.0")
      .config("spark.local.ip", localIp)
      .config("spark.sql.shuffle.partitions", "1")
      .config("spark.default.parallelism", "1")
      .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .config("spark.sql.catalog.iceberg_catalog", "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.iceberg_catalog.type", "hadoop")
      .config("spark.sql.catalog.iceberg_catalog.warehouse", warehouse)
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    try {
      val startAllNs = System.nanoTime()

      spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg_catalog.test_db")

      val srcDf = Seq(
        (1L, "u001", BigDecimal("18.50"), "2026-03-19 10:00:00"),
        (2L, "u002", BigDecimal("20.00"), "2026-03-19 10:05:00"),
        (3L, "u003", BigDecimal("99.99"), "2026-03-19 10:10:00")
      ).toDF("order_id", "user_id", "amount", "created_at")
        .withColumn("created_at", col("created_at").cast("timestamp"))

      srcDf.writeTo("iceberg_catalog.test_db.yamtrix_test").createOrReplace()

      println("===== 查询 Iceberg 表 =====")
      val icebergDf = spark.sql("SELECT * FROM iceberg_catalog.test_db.yamtrix_test")
      icebergDf.show(false)

      val saveStartNs = System.nanoTime()
      icebergDf
        .repartition(1)
        .write
        .format("its-ymatrix")
        .option("url", ymatrixUrl)
        .option("user", ymatrixUser)
        .option("password", ymatrixPassword)
        .option("dbschema", ymatrixSchema)
        .option("dbtable", targetTable)
        .option("distributedby", "order_id")
        .option("server.port", "43000")
        .option("network.timeout", ymatrixNetworkTimeout)
        .option("server.timeout", ymatrixServerTimeout)
        .option("dbmessages", ymatrixDbMessages)
        .mode("append")
        .save()
      val saveElapsedMs = (System.nanoTime() - saveStartNs) / 1000000L

      val verifyDf = spark.read
        .format("its-ymatrix")
        .option("url", ymatrixUrl)
        .option("user", ymatrixUser)
        .option("password", ymatrixPassword)
        .option("dbtable", s"select count(*)::bigint as row_count from $ymatrixSchema.$targetTable")
        .load()

      val rowCount = verifyDf.first().getAs[Long]("row_count")
      val totalElapsedMs = (System.nanoTime() - startAllNs) / 1000000L

      println(s"[result] ymatrix_table=$ymatrixSchema.$targetTable")
      println(s"[result] save_elapsed_ms=$saveElapsedMs")
      println(s"[result] total_elapsed_ms=$totalElapsedMs")
      println(s"[result] row_count=$rowCount")
    } finally {
      spark.stop()
    }
  }
}
