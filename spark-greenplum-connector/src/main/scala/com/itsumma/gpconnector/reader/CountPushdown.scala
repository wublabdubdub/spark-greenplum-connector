package com.itsumma.gpconnector.reader

import org.apache.spark.sql.connector.expressions.aggregate.{Aggregation, CountStar}
import org.apache.spark.sql.types.{LongType, StructField, StructType}

private[gpconnector] final case class CountPushdown(outputSchema: StructType)

private[gpconnector] object CountPushdown {
  val OutputSchema: StructType = StructType(Seq(
    StructField("connector_count", LongType, nullable = false)
  ))

  def accept(
      aggregation: Aggregation,
      allFiltersPushed: Boolean,
      sqlTransfer: String): Option[CountPushdown] = {
    val aggregateExpressions = aggregation.aggregateExpressions()
    val groupingExpressions = aggregation.groupByExpressions()
    val supported =
      allFiltersPushed &&
        Option(sqlTransfer).forall(_.trim.isEmpty) &&
        groupingExpressions.isEmpty &&
        aggregateExpressions.length == 1 &&
        aggregateExpressions.head.isInstanceOf[CountStar]
    if (supported) Some(CountPushdown(OutputSchema)) else None
  }
}
