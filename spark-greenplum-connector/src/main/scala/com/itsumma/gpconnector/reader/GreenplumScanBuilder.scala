package com.itsumma.gpconnector.reader

import com.itsumma.gpconnector.GreenplumRowSet
import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.expressions.aggregate.Aggregation
import org.apache.spark.sql.connector.read.{Scan, ScanBuilder, SupportsPushDownAggregates, SupportsPushDownFilters, SupportsPushDownRequiredColumns}
import org.apache.spark.sql.itsumma.gpconnector.{GPOptionsFactory, SparkSchemaUtil}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType

class GreenplumScanBuilder(
    optionsFactory: GPOptionsFactory,
    rowSet: GreenplumRowSet,
    sourceSchema: StructType)
  extends ScanBuilder
    with SupportsPushDownFilters
    with SupportsPushDownRequiredColumns
    with SupportsPushDownAggregates
    with Logging
{
  private var outputSchema = sourceSchema
  private var whereClause = ""
  private var pushedDownFilters: Array[Filter] = Array.empty[Filter]
  private var unsupportedFilters: Array[Filter] = Array.empty[Filter]
  private var countPushdown: Option[CountPushdown] = None
  logDebug(s"""options=\n${optionsFactory.dumpParams()}""")
  private var builtScan: Scan = null

  override def build(): Scan = this.synchronized {
    if (builtScan == null) {
      builtScan = countPushdown match {
        case Some(_) =>
          new GreenplumCountScan(
            optionsFactory,
            optionsFactory.tableOrQuery,
            whereClause)
        case None =>
          new GreenplumScan(
            optionsFactory,
            rowSet,
            sourceSchema,
            outputSchema,
            whereClause)
      }
    }
    builtScan
  }

  override def pushFilters(filters: Array[Filter]): Array[Filter] = {
    val tuple3 = SparkSchemaUtil(optionsFactory.dbTimezone).pushFilters(filters)
    whereClause = tuple3._1
    unsupportedFilters = tuple3._2
    pushedDownFilters = tuple3._3
    unsupportedFilters
  }

  override def pushedFilters(): Array[Filter] = pushedDownFilters

  override def pruneColumns(requiredSchema: StructType): Unit = {
    outputSchema = requiredSchema
  }

  override def pushAggregation(aggregation: Aggregation): Boolean = {
    countPushdown = CountPushdown.accept(
      aggregation,
      allFiltersPushed = unsupportedFilters.isEmpty,
      sqlTransfer = optionsFactory.sqlTransfer)
    countPushdown.nonEmpty
  }

  override def supportCompletePushDown(
      aggregation: Aggregation): Boolean =
    CountPushdown.accept(
      aggregation,
      allFiltersPushed = unsupportedFilters.isEmpty,
      sqlTransfer = optionsFactory.sqlTransfer).nonEmpty

  private[reader] def currentOutputSchema: StructType = outputSchema

  private[reader] def currentUnsupportedFilters: Array[Filter] =
    unsupportedFilters.clone()

  private[reader] def hasCountPushdown: Boolean = countPushdown.nonEmpty
}
