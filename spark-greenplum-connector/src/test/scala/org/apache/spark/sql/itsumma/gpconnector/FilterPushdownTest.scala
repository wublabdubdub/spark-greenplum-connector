package org.apache.spark.sql.itsumma.gpconnector

import org.apache.spark.sql.sources.{Filter, GreaterThan, Or}

object FilterPushdownTest {
  private final case class UnsupportedFilter(name: String) extends Filter

  def main(args: Array[String]): Unit = {
    val util = SparkSchemaUtil("Asia/Shanghai")
    val supported = GreaterThan("amount", BigDecimal(50))
    val unsupported = UnsupportedFilter("x")

    val (where, rejected, accepted) = util.pushFilters(Array(supported, unsupported))
    assert(where.contains("amount"))
    assert(!where.contains("UnsupportedFilter"))
    assert(rejected.sameElements(Array(unsupported)))
    assert(accepted.sameElements(Array(supported)))

    val (_, rejectedOr, acceptedOr) =
      util.pushFilters(Array(Or(supported, unsupported)))
    assert(rejectedOr.length == 1)
    assert(acceptedOr.isEmpty)
    println("FILTER_PUSHDOWN_TEST_OK")
  }
}
