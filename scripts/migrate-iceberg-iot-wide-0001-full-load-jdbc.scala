import java.sql.DriverManager

import org.apache.spark.sql.types._

import scala.util.control.NonFatal

val icebergTable = "local.test_db.iot_wide_0001"

val gpCatalog = "gp"
val gpUrl = "jdbc:postgresql://172.16.100.29:5432/zhangchen"
val gpUser = "zhangchen"
val gpPassword = "YMatrix@123"
val gpSchema = "public"
val gpTable = "iot_wide_0001"

val writePartitions = 8
val shufflePartitions = 8
val distributedBy = "ingest_id"

case class TableProfile(rowCount: Long, minIngestId: Long, maxIngestId: Long)

def quoteIdent(ident: String): String = "\"" + ident.replace("\"", "\"\"") + "\""

def quoteSparkIdent(ident: String): String = "`" + ident.replace("`", "``") + "`"

def quoteSparkMultipart(name: String): String = {
  name.split("\\.").map(quoteSparkIdent).mkString(".")
}

def qualifiedTableName(schema: String, table: String): String = {
  s"${quoteIdent(schema)}.${quoteIdent(table)}"
}

def qualifiedCatalogTableName(catalog: String, schema: String, table: String): String = {
  s"${quoteSparkIdent(catalog)}.${quoteSparkIdent(schema)}.${quoteSparkIdent(table)}"
}

def gpType(dataType: DataType): String = dataType match {
  case LongType => "bigint"
  case IntegerType => "integer"
  case DoubleType => "double precision"
  case FloatType => "real"
  case ShortType => "smallint"
  case ByteType => "smallint"
  case BooleanType => "boolean"
  case StringType => "text"
  case DateType => "date"
  case TimestampType => "timestamp"
  case d: DecimalType => s"numeric(${d.precision},${d.scale})"
  case _ => throw new IllegalArgumentException(s"Unsupported Spark type for Greenplum JDBC DDL: ${dataType.simpleString}")
}

def withGpConnection[T](f: java.sql.Connection => T): T = {
  val conn = DriverManager.getConnection(gpUrl, gpUser, gpPassword)
  try f(conn)
  finally conn.close()
}

def probeGp(): Unit = {
  withGpConnection { conn =>
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
  }
}

def ensureTargetTable(schema: StructType): Unit = {
  val columnDefs = schema.fields.map { field =>
    val nullableClause = if (field.nullable) "" else " not null"
    s"${quoteIdent(field.name)} ${gpType(field.dataType)}${nullableClause}"
  }.mkString(",\n  ")

  val createSql =
    s"""
       |create schema if not exists ${quoteIdent(gpSchema)};
       |drop table if exists ${qualifiedTableName(gpSchema, gpTable)};
       |create table ${qualifiedTableName(gpSchema, gpTable)} (
       |  ${columnDefs}
       |)
       |distributed by (${quoteIdent(distributedBy)})
       |""".stripMargin

  withGpConnection { conn =>
    val stmt = conn.createStatement()
    try {
      createSql
        .split(";")
        .map(_.trim)
        .filter(_.nonEmpty)
        .foreach(stmt.execute)
    } finally {
      stmt.close()
    }
  }
}

def ensureGpCatalogConfigured(): Unit = {
  val requiredConf = Map(
    s"spark.sql.catalog.${gpCatalog}" -> "org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCTableCatalog",
    s"spark.sql.catalog.${gpCatalog}.url" -> gpUrl,
    s"spark.sql.catalog.${gpCatalog}.user" -> gpUser,
    s"spark.sql.catalog.${gpCatalog}.password" -> gpPassword,
    s"spark.sql.catalog.${gpCatalog}.driver" -> "org.postgresql.Driver"
  )

  requiredConf.foreach { case (key, expectedValue) =>
    val actualValue = spark.conf.getOption(key).getOrElse("")
    require(
      actualValue == expectedValue,
      s"Missing or unexpected Spark config: ${key}. expected=${expectedValue}, actual=${actualValue}"
    )
  }
}

def profileIcebergTable(): TableProfile = {
  val metrics = spark.sql(
    s"""
       |select
       |  cast(count(1) as bigint) as row_count,
       |  coalesce(cast(min(ingest_id) as bigint), cast(0 as bigint)) as min_ingest_id,
       |  coalesce(cast(max(ingest_id) as bigint), cast(0 as bigint)) as max_ingest_id
       |from ${quoteSparkMultipart(icebergTable)}
       |""".stripMargin
  ).head()

  TableProfile(
    metrics.getAs[Long]("row_count"),
    metrics.getAs[Long]("min_ingest_id"),
    metrics.getAs[Long]("max_ingest_id")
  )
}

def queryGpProfile(): TableProfile = {
  withGpConnection { conn =>
    val stmt = conn.prepareStatement(
      s"""
         |select
         |  count(*)::bigint as row_count,
         |  coalesce(min(ingest_id), 0)::bigint as min_ingest_id,
         |  coalesce(max(ingest_id), 0)::bigint as max_ingest_id
         |from ${qualifiedTableName(gpSchema, gpTable)}
         |""".stripMargin
    )
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
  }
}

spark.conf.set("spark.sql.shuffle.partitions", shufflePartitions.toString)

val exitCode = try {
  println(
    s"""
       |[config] icebergTable=${icebergTable}
       |[config] gpCatalog=${gpCatalog}
       |[config] gpTarget=${gpSchema}.${gpTable}
       |[config] writePartitions=${writePartitions}
       |[config] shufflePartitions=${shufflePartitions}
       |[config] distributedBy=${distributedBy}
       |[config] mode=Spark SQL insert into JDBC catalog table
       |""".stripMargin
  )

  ensureGpCatalogConfigured()

  val sourceDf = spark.sql(s"select * from ${quoteSparkMultipart(icebergTable)}")
  println(s"[schema] ${sourceDf.schema.treeString}")

  val sourceProfile = profileIcebergTable()
  println(
    s"[source] rowCount=${sourceProfile.rowCount}, " +
      s"minIngestId=${sourceProfile.minIngestId}, maxIngestId=${sourceProfile.maxIngestId}"
  )

  require(sourceProfile.rowCount > 0L, s"${icebergTable} is empty")

  probeGp()
  println("[probe] Greenplum JDBC probe passed")

  ensureTargetTable(sourceDf.schema)
  println(s"[ddl] Recreated ${gpSchema}.${gpTable} with DISTRIBUTED BY (${distributedBy})")

  val catalogTargetTable = qualifiedCatalogTableName(gpCatalog, gpSchema, gpTable)
  val insertSql =
    s"""
       |insert into ${catalogTargetTable}
       |select /*+ REPARTITION(${writePartitions}, ${quoteSparkIdent(distributedBy)}) */
       |  *
       |from ${quoteSparkMultipart(icebergTable)}
       |""".stripMargin

  println(s"[sql] Running Spark SQL insert-select from ${icebergTable} to ${catalogTargetTable}")
  spark.sql(insertSql)

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
      s"to ${catalogTargetTable} through Spark SQL + JDBC catalog"
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
