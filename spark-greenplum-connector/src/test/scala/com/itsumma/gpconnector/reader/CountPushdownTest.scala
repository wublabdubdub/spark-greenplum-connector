package com.itsumma.gpconnector.reader

import org.apache.spark.sql.connector.expressions.{Expression, Expressions}
import org.apache.spark.sql.connector.expressions.aggregate._
import org.apache.spark.sql.types.LongType

object CountPushdownTest {
  def main(args: Array[String]): Unit = {
    val count = new Aggregation(
      Array[AggregateFunc](new CountStar()),
      Array.empty[Expression])
    val groupedCount = new Aggregation(
      Array[AggregateFunc](new CountStar()),
      Array[Expression](Expressions.column("order_id")))
    val nonCount = new Aggregation(
      Array[AggregateFunc](new Max(Expressions.column("amount"))),
      Array.empty[Expression])

    assert(CountPushdown.accept(
      count, allFiltersPushed = true, sqlTransfer = "").nonEmpty)
    assert(CountPushdown.accept(
      count, allFiltersPushed = false, sqlTransfer = "").isEmpty)
    assert(CountPushdown.accept(
      count, allFiltersPushed = true, sqlTransfer = "custom").isEmpty)
    assert(CountPushdown.accept(
      groupedCount, allFiltersPushed = true, sqlTransfer = "").isEmpty)
    assert(CountPushdown.accept(
      nonCount, allFiltersPushed = true, sqlTransfer = "").isEmpty)
    assert(CountPushdown.OutputSchema.fields.length == 1)
    assert(CountPushdown.OutputSchema.fields.head.dataType == LongType)
    assert(!CountPushdown.OutputSchema.fields.head.nullable)
    println("COUNT_PUSHDOWN_TEST_OK")
  }
}
