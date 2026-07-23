package com.itsumma.gpconnector.reader

import com.itsumma.gpconnector.GPClient
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

private[gpconnector] final class GreenplumCountScan(
    optionsFactory: GPOptionsFactory,
    tableOrQuery: String,
    whereClause: String)
  extends Scan
    with Batch {

  override def readSchema(): StructType = CountPushdown.OutputSchema

  override def toBatch: Batch = this

  override def planInputPartitions(): Array[InputPartition] =
    Array(GreenplumCountInputPartition)

  override def createReaderFactory(): PartitionReaderFactory =
    GreenplumCountReaderFactory(optionsFactory, tableOrQuery, whereClause)
}

private[gpconnector] case object GreenplumCountInputPartition
  extends InputPartition

private[gpconnector] final case class GreenplumCountReaderFactory(
    optionsFactory: GPOptionsFactory,
    tableOrQuery: String,
    whereClause: String)
  extends PartitionReaderFactory {

  override def createReader(
      partition: InputPartition): PartitionReader[InternalRow] = {
    require(
      partition == GreenplumCountInputPartition,
      s"Unexpected COUNT input partition ${partition.getClass.getName}")
    new GreenplumCountPartitionReader(
      optionsFactory,
      tableOrQuery,
      whereClause)
  }
}

private[gpconnector] final class GreenplumCountPartitionReader(
    optionsFactory: GPOptionsFactory,
    tableOrQuery: String,
    whereClause: String,
    suppliedConnection: Connection = null,
    suppliedDefaultSchema: String = null)
  extends PartitionReader[InternalRow] {

  private var gpClient: GPClient = null
  private var connection: Connection = suppliedConnection
  private var statement: PreparedStatement = null
  private var resultSet: ResultSet = null
  private var executed = false
  private var currentRow: InternalRow = null

  override def next(): Boolean = {
    if (executed) {
      currentRow = null
      return false
    }
    executed = true
    try {
      val defaultSchema =
        if (connection != null) suppliedDefaultSchema
        else {
          gpClient = new GPClient(optionsFactory)
          connection = gpClient.getConnection()
          GPClient.checkDbObjSearchPath(
            connection,
            optionsFactory.dbSchema)
        }
      val sql = CountSql.build(tableOrQuery, whereClause, defaultSchema)
      statement = connection.prepareStatement(sql)
      resultSet = statement.executeQuery()
      if (!resultSet.next()) {
        throw new IllegalStateException(
          s"COUNT query returned no row: $sql")
      }
      val count = resultSet.getLong(1)
      if (resultSet.wasNull()) {
        throw new IllegalStateException(
          s"COUNT query returned NULL: $sql")
      }
      currentRow = new GenericInternalRow(Array[Any](count))
      true
    } catch {
      case failure: Throwable =>
        try close()
        catch {
          case cleanupFailure: Throwable =>
            failure.addSuppressed(cleanupFailure)
        }
        throw failure
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
    var firstFailure: Throwable = null
    def cleanup(body: => Unit): Unit = {
      try body
      catch {
        case failure: Throwable =>
          if (firstFailure == null) firstFailure = failure
          else firstFailure.addSuppressed(failure)
      }
    }
    if (resultSet != null) {
      cleanup(resultSet.close())
      resultSet = null
    }
    if (statement != null) {
      cleanup(statement.close())
      statement = null
    }
    if (connection != null) {
      cleanup(connection.close())
      connection = null
    }
    if (gpClient != null) {
      cleanup(gpClient.close())
      gpClient = null
    }
    if (firstFailure != null) throw firstFailure
  }
}
