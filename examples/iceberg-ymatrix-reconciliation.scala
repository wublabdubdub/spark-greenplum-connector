// 导入 DriverManager，用于直接通过 JDBC 访问 YMatrix 元数据。
import java.sql.DriverManager

// 导入 DataFrame 类型。
import org.apache.spark.sql.DataFrame
// 导入 Spark SQL 常用函数。
import org.apache.spark.sql.functions._

/**
 * 这个示例建议通过 spark-shell 运行。
 *
 * scala> :load examples/iceberg-ymatrix-reconciliation.scala
 *
 * 这个脚本用于第二阶段压测验收：
 * 1. 对比 Iceberg 与 YMatrix 各宽表的 count/min/max ingest_id
 * 2. 可选地抽样比对指定 ingest_id 对应的整行内容
 * 3. 输出通过、失败和跳过的表清单
 */

// 定义 Iceberg namespace。
val icebergNamespace = "local.test_db"
// 定义 Iceberg 表名前缀。
val icebergTablePrefix = "iot_wide_"

// 定义 YMatrix JDBC 地址；如已设置环境变量，则优先使用环境变量。
val ymatrixUrl = sys.env.getOrElse("YMATRIX_URL", "jdbc:postgresql://172.16.100.29:5432/zhangchen")
// 定义 YMatrix 用户名。
val ymatrixUser = sys.env.getOrElse("YMATRIX_USER", "zhangchen")
// 定义 YMatrix 密码。
val ymatrixPassword = sys.env.getOrElse("YMATRIX_PASSWORD", "YMatrix@123")
// 定义 YMatrix schema。
val ymatrixSchema = sys.env.getOrElse("YMATRIX_SCHEMA", "public")
// 定义 YMatrix 目标表名前缀。
val ymatrixTablePrefix = "iot_wide_"

// 定义起始表号。
val tableStartIndex = 1
// 定义要核对的表数量。
val tableCount = 10
// 定义抽样校验的 ingest_id 数量。
val sampleIdCount = 3
// 定义是否启用整行抽样校验。
val includeRowSampleCheck = true

// 校验表数量必须为正数。
require(tableCount > 0, "tableCount must be positive")
// 校验抽样数量不能为负数。
require(sampleIdCount >= 0, "sampleIdCount must be >= 0")

// 定义宽表字段顺序，用于抽样时做稳定的 JSON 对比。
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

// 用简单结构保存表统计信息。
case class TableStats(rowCount: Long, minIngestId: Long, maxIngestId: Long)

// 根据表号拼出 Iceberg 全表名。
def icebergTableName(tableNo: Int): String = {
  f"${icebergNamespace}.${icebergTablePrefix}${tableNo}%04d"
}

// 根据表号拼出 YMatrix 目标表名。
def ymatrixTableName(tableNo: Int): String = {
  f"${ymatrixTablePrefix}${tableNo}%04d"
}

// 封装一个通用 JDBC 连接函数。
def withYMatrixConnection[T](f: java.sql.Connection => T): T = {
  val conn = DriverManager.getConnection(ymatrixUrl, ymatrixUser, ymatrixPassword)
  try f(conn)
  finally conn.close()
}

// 为 SQL 标识符加双引号。
def quoteIdent(ident: String): String = {
  "\"" + ident.replace("\"", "\"\"") + "\""
}

// 判断 YMatrix 目标表是否已经存在。
def ymatrixTableExists(tableName: String): Boolean = {
  withYMatrixConnection { conn =>
    val stmt = conn.prepareStatement(
      """
        |select count(*)::bigint
        |from pg_catalog.pg_tables
        |where schemaname = ?
        |  and tablename = ?
        |""".stripMargin
    )
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

// 通过 its-ymatrix connector 执行查询并返回 DataFrame。
def ymatrixRead(query: String): DataFrame = {
  spark.read
    .format("its-ymatrix")
    .option("url", ymatrixUrl)
    .option("user", ymatrixUser)
    .option("password", ymatrixPassword)
    .option("dbtable", query)
    .load()
}

// 从单行统计结果中取出 long 值。
def rowLong(row: org.apache.spark.sql.Row, index: Int): Long = {
  if (row == null || row.isNullAt(index)) 0L else row.getLong(index)
}

// 读取 Iceberg 源表统计信息。
def sourceStats(fullTableName: String): TableStats = {
  val row = spark.table(fullTableName)
    .agg(
      count(lit(1)).cast("bigint").as("row_count"),
      coalesce(min(col("ingest_id")), lit(0L)).cast("bigint").as("min_ingest_id"),
      coalesce(max(col("ingest_id")), lit(0L)).cast("bigint").as("max_ingest_id")
    )
    .head()

  TableStats(
    rowLong(row, 0),
    rowLong(row, 1),
    rowLong(row, 2)
  )
}

// 读取 YMatrix 目标表统计信息。
def targetStats(tableName: String): TableStats = {
  if (!ymatrixTableExists(tableName)) {
    TableStats(0L, 0L, 0L)
  } else {
    val qualifiedTable = s"${quoteIdent(ymatrixSchema)}.${quoteIdent(tableName)}"
    val row = ymatrixRead(
      s"""
         |select
         |  count(*)::bigint as row_count,
         |  coalesce(min(ingest_id), 0)::bigint as min_ingest_id,
         |  coalesce(max(ingest_id), 0)::bigint as max_ingest_id
         |from ${qualifiedTable}
         |""".stripMargin
    ).head()

    TableStats(
      rowLong(row, 0),
      rowLong(row, 1),
      rowLong(row, 2)
    )
  }
}

// 生成用于抽样比对的 ingest_id 列表。
def buildSampleIds(minId: Long, maxId: Long, sampleCount: Int): Seq[Long] = {
  if (sampleCount <= 0 || minId <= 0L || maxId <= 0L || maxId < minId) {
    Seq.empty
  } else if (sampleCount == 1) {
    Seq(maxId)
  } else {
    (0 until sampleCount)
      .map(i => minId + ((maxId - minId) * i) / (sampleCount - 1))
      .distinct
      .sorted
  }
}

// 把 DataFrame 抽样行转成 ingest_id -> JSON 的 Map。
def collectCanonicalRows(df: DataFrame): Map[Long, String] = {
  df.select(
    col("ingest_id").cast("bigint").as("ingest_id"),
    to_json(struct(wideColumns.map(col): _*)).as("payload")
  ).collect().map { row =>
    row.getLong(0) -> row.getString(1)
  }.toMap
}

// 读取 Iceberg 抽样行。
def loadSourceSample(fullTableName: String, sampleIds: Seq[Long]): Map[Long, String] = {
  if (sampleIds.isEmpty) {
    Map.empty
  } else {
    val ids = sampleIds.map(Long.box)
    collectCanonicalRows(
      spark.table(fullTableName)
        .where(col("ingest_id").isin(ids: _*))
        .select(wideColumns.map(col): _*)
        .orderBy(col("ingest_id"))
    )
  }
}

// 读取 YMatrix 抽样行。
def loadTargetSample(tableName: String, sampleIds: Seq[Long]): Map[Long, String] = {
  if (sampleIds.isEmpty || !ymatrixTableExists(tableName)) {
    Map.empty
  } else {
    val qualifiedTable = s"${quoteIdent(ymatrixSchema)}.${quoteIdent(tableName)}"
    val sampleIdSql = sampleIds.sorted.mkString(", ")
    val query =
      s"""
         |select ${wideColumns.mkString(", ")}
         |from ${qualifiedTable}
         |where ingest_id in (${sampleIdSql})
         |order by ingest_id
         |""".stripMargin

    collectCanonicalRows(ymatrixRead(query))
  }
}

// 对账指定编号的一张表；返回 pass、fail 或 skip。
def reconcileTable(tableNo: Int): String = {
  val sourceTable = icebergTableName(tableNo)
  val targetTable = ymatrixTableName(tableNo)

  if (!spark.catalog.tableExists(sourceTable)) {
    println(s"[skip] ${sourceTable} does not exist")
    "skip"
  } else {
    val srcStats = sourceStats(sourceTable)
    val dstStats = targetStats(targetTable)
    val statsMatched = srcStats == dstStats

    val sampleIds =
      if (includeRowSampleCheck && statsMatched && srcStats.rowCount > 0L) {
        buildSampleIds(srcStats.minIngestId, srcStats.maxIngestId, sampleIdCount)
      } else {
        Seq.empty[Long]
      }

    val sampleMatched =
      if (!includeRowSampleCheck || sampleIds.isEmpty) {
        true
      } else {
        loadSourceSample(sourceTable, sampleIds) == loadTargetSample(targetTable, sampleIds)
      }

    if (statsMatched && sampleMatched) {
      println(
        s"[pass] ${sourceTable} == ${ymatrixSchema}.${targetTable}, " +
          s"rowCount=${srcStats.rowCount}, ingest_id=[${srcStats.minIngestId}, ${srcStats.maxIngestId}]"
      )
      "pass"
    } else {
      println(
        s"[fail] ${sourceTable} != ${ymatrixSchema}.${targetTable}, " +
          s"source={rowCount=${srcStats.rowCount}, min=${srcStats.minIngestId}, max=${srcStats.maxIngestId}}, " +
          s"target={rowCount=${dstStats.rowCount}, min=${dstStats.minIngestId}, max=${dstStats.maxIngestId}}"
      )

      if (includeRowSampleCheck && sampleIds.nonEmpty && !sampleMatched) {
        println(s"[fail] sample rows mismatch, sampleIds=${sampleIds.mkString(",")}")
      }

      "fail"
    }
  }
}

// 打印对账启动配置。
println(
  s"""
     |Iceberg / YMatrix reconciliation started.
     |icebergNamespace=${icebergNamespace}
     |icebergTablePrefix=${icebergTablePrefix}
     |ymatrixSchema=${ymatrixSchema}
     |ymatrixTablePrefix=${ymatrixTablePrefix}
     |tableStartIndex=${tableStartIndex}
     |tableCount=${tableCount}
     |sampleIdCount=${sampleIdCount}
     |includeRowSampleCheck=${includeRowSampleCheck}
     |""".stripMargin
)

// 执行批量对账，并统计结果。
val reconcileResults = (tableStartIndex until tableStartIndex + tableCount)
  .map(tableNo => reconcileTable(tableNo))

val passCount = reconcileResults.count(_ == "pass")
val failCount = reconcileResults.count(_ == "fail")
val skipCount = reconcileResults.count(_ == "skip")

println(
  s"""
     |Reconciliation finished.
     |passCount=${passCount}
     |failCount=${failCount}
     |skipCount=${skipCount}
     |""".stripMargin
)
