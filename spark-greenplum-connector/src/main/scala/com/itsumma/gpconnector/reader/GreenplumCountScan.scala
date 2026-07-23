package com.itsumma.gpconnector.reader

import com.itsumma.gpconnector.{GPClient, GreenplumRowSet}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.itsumma.gpconnector.{GPOptionsFactory, GPTarget}
import org.apache.spark.sql.types.StructType

import java.sql.{Connection, PreparedStatement, ResultSet}

private[gpconnector] object CountSql {
  def build(
      tableOrQuery: String,
      whereClause: String,
      defaultSchema: String): String = {
    val target = GPTarget(tableOrQuery)
    val fromClause =
      if (target.isQuery) {
        s"(${tableOrQuery.trim}) connector_count_source"
      } else {
        val canonicalName = target.getCanonicalName(defaultSchema)
        require(
          canonicalName.nonEmpty,
          s"Unable to resolve COUNT source from dbtable=$tableOrQuery")
        canonicalName
      }
    val filter =
      Option(whereClause).map(_.trim).filter(_.nonEmpty)
        .map(sql => s" where $sql")
        .getOrElse("")
    s"select count(*)::bigint as connector_count from $fromClause$filter"
  }
}

private[gpconnector] object DriverCountQuery {
  def execute(
      optionsFactory: GPOptionsFactory,
      rowSet: GreenplumRowSet,
      tableOrQuery: String,
      whereClause: String): Long = {
    require(rowSet != null, "GreenplumRowSet is required for COUNT pushdown")
    val connection = rowSet.getGpClient.getConnection()
    var count = 0L
    var failure: Throwable = null
    try {
      val defaultSchema =
        GPClient.checkDbObjSearchPath(connection, optionsFactory.dbSchema)
      count = execute(connection, tableOrQuery, whereClause, defaultSchema)
    } catch {
      case caught: Throwable =>
        failure = caught
    } finally {
      failure = cleanup(failure, connection.close())
    }
    if (failure != null) throw failure
    count
  }

  def execute(
      connection: Connection,
      tableOrQuery: String,
      whereClause: String,
      defaultSchema: String): Long = {
    val sql = CountSql.build(tableOrQuery, whereClause, defaultSchema)
    var statement: PreparedStatement = null
    var resultSet: ResultSet = null
    var count = 0L
    var failure: Throwable = null
    try {
      statement = connection.prepareStatement(sql)
      resultSet = statement.executeQuery()
      if (!resultSet.next()) {
        throw new IllegalStateException(
          s"COUNT query returned no row: $sql")
      }
      count = resultSet.getLong(1)
      if (resultSet.wasNull()) {
        throw new IllegalStateException(
          s"COUNT query returned NULL: $sql")
      }
    } catch {
      case caught: Throwable =>
        failure = caught
    } finally {
      if (resultSet != null) {
        failure = cleanup(failure, resultSet.close())
      }
      if (statement != null) {
        failure = cleanup(failure, statement.close())
      }
    }
    if (failure != null) throw failure
    count
  }

  private def cleanup(
      existingFailure: Throwable,
      body: => Unit): Throwable = {
    try {
      body
      existingFailure
    } catch {
      case cleanupFailure: Throwable =>
        if (existingFailure == null) cleanupFailure
        else {
          existingFailure.addSuppressed(cleanupFailure)
          existingFailure
        }
    }
  }
}

private[gpconnector] final class GreenplumCountScan(
    optionsFactory: GPOptionsFactory,
    rowSet: GreenplumRowSet,
    tableOrQuery: String,
    whereClause: String,
    suppliedCountLoader: () => Long = null)
  extends Scan
    with Batch {

  private lazy val countValue: Long =
    if (suppliedCountLoader != null) suppliedCountLoader()
    else {
      DriverCountQuery.execute(
        optionsFactory,
        rowSet,
        tableOrQuery,
        whereClause)
    }

  override def readSchema(): StructType = CountPushdown.OutputSchema

  override def toBatch: Batch = this

  override def planInputPartitions(): Array[InputPartition] =
    Array(GreenplumCountInputPartition(countValue))

  override def createReaderFactory(): PartitionReaderFactory =
    GreenplumCountReaderFactory
}

private[gpconnector] final case class GreenplumCountInputPartition(
    countValue: Long)
  extends InputPartition

private[gpconnector] case object GreenplumCountReaderFactory
  extends PartitionReaderFactory {

  override def createReader(
      partition: InputPartition): PartitionReader[InternalRow] =
    partition match {
      case countPartition: GreenplumCountInputPartition =>
        new GreenplumCountPartitionReader(countPartition.countValue)
      case unexpected =>
        throw new IllegalArgumentException(
          s"Unexpected COUNT input partition ${unexpected.getClass.getName}")
    }
}

private[gpconnector] final class GreenplumCountPartitionReader(
    countValue: Long)
  extends PartitionReader[InternalRow] {

  private val countRow =
    new GenericInternalRow(Array[Any](countValue))
  private var emitted = false
  private var currentRow: InternalRow = null

  override def next(): Boolean = {
    if (emitted) {
      currentRow = null
      false
    } else {
      emitted = true
      currentRow = countRow
      true
    }
  }

  override def get(): InternalRow = {
    if (currentRow == null) {
      throw new IllegalStateException(
        "get() called without a current COUNT row")
    }
    currentRow
  }

  override def close(): Unit = {
    currentRow = null
  }
}
