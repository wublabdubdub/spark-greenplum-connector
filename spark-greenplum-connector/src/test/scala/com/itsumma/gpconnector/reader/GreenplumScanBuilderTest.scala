package com.itsumma.gpconnector.reader

import org.apache.spark.sql.connector.expressions.Expression
import org.apache.spark.sql.connector.expressions.aggregate.{AggregateFunc, Aggregation, CountStar}
import org.apache.spark.sql.itsumma.gpconnector.GPOptionsFactory
import org.apache.spark.sql.sources.{EqualNullSafe, GreaterThan}
import org.apache.spark.sql.types.{LongType, StructField, StructType}

object GreenplumScanBuilderTest {
  def main(args: Array[String]): Unit = {
    val options = GPOptionsFactory(Map(
      "url" -> "jdbc:postgresql://localhost/test",
      "dbtable" -> "cdm_dwyz.orders"
    ))
    val sourceSchema = StructType(Seq(
      StructField("order_id", LongType),
      StructField("amount", LongType)
    ))
    val builder = new GreenplumScanBuilder(options, null, sourceSchema)
    val outputSchema = StructType(Seq(sourceSchema("amount")))
    builder.pruneColumns(outputSchema)
    assert(builder.currentOutputSchema == outputSchema)
    assert(sourceSchema.fieldNames.sameElements(Array("order_id", "amount")))

    assert(builder.pushFilters(Array(GreaterThan("amount", 50L))).isEmpty)
    val count = new Aggregation(
      Array[AggregateFunc](new CountStar()),
      Array.empty[Expression])
    assert(builder.pushAggregation(count))
    assert(builder.supportCompletePushDown(count))
    assert(builder.hasCountPushdown)
    assert(builder.build().isInstanceOf[GreenplumCountScan])

    val unsupportedBuilder =
      new GreenplumScanBuilder(options, null, sourceSchema)
    val unsupported = EqualNullSafe("x", 1)
    assert(unsupportedBuilder.pushFilters(Array(unsupported))
      .sameElements(Array(unsupported)))
    assert(!unsupportedBuilder.pushAggregation(count))
    assert(!unsupportedBuilder.hasCountPushdown)
    println("GREENPLUM_SCAN_BUILDER_TEST_OK")
  }
}
