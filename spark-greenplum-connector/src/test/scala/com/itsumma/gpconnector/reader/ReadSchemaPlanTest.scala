package com.itsumma.gpconnector.reader

import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.types._

object ReadSchemaPlanTest {
  def main(args: Array[String]): Unit = {
    val source = StructType(Seq(
      StructField("order_id", LongType),
      StructField("amount", DecimalType(10, 2)),
      StructField("CaseSensitiveKey", StringType)
    ))
    val amountOnly = StructType(Seq(source("amount")))

    val plan = ReadSchemaPlan.build(
      source,
      amountOnly,
      "distributed by (order_id)",
      "order_id")
    assert(plan.outputSchema == amountOnly)
    assert(plan.transferSchema.fieldNames.sameElements(
      Array("amount", "order_id")))
    assert(plan.distributionClause == "distributed by (order_id)")
    assert(plan.unresolvedDistributionColumns.isEmpty)

    val alreadyPresent = ReadSchemaPlan.build(
      source,
      StructType(Seq(source("order_id"), source("amount"))),
      "distributed by (order_id)",
      "order_id")
    assert(alreadyPresent.transferSchema.fieldNames.sameElements(
      Array("order_id", "amount")))

    val quoted = ReadSchemaPlan.build(
      source,
      amountOnly,
      """distributed by ("CaseSensitiveKey")""",
      "\"CaseSensitiveKey\"")
    assert(quoted.transferSchema.fieldNames.sameElements(
      Array("amount", "CaseSensitiveKey")))

    val unresolved = ReadSchemaPlan.build(
      amountOnly,
      amountOnly,
      "distributed by (order_id)",
      "order_id")
    assert(unresolved.transferSchema == amountOnly)
    assert(unresolved.distributionClause == "distributed randomly")
    assert(unresolved.unresolvedDistributionColumns == Seq("order_id"))

    val multiColumn = ReadSchemaPlan.build(
      source,
      amountOnly,
      "distributed by (order_id, \"CaseSensitiveKey\")",
      "order_id,\"CaseSensitiveKey\"")
    assert(multiColumn.transferSchema.fieldNames.sameElements(
      Array("amount", "order_id", "CaseSensitiveKey")))

    val transferRow = new GenericInternalRow(Array[Any](
      org.apache.spark.sql.types.Decimal(BigDecimal("12.34"), 10, 2),
      42L))
    val projected = plan.projector.project(transferRow)
    assert(projected.numFields == 1)
    assert(projected.getDecimal(0, 10, 2).toString == "12.34")

    val emptyOutput = ReadSchemaPlan.build(
      source,
      new StructType(),
      "distributed by (order_id)",
      "order_id")
    assert(emptyOutput.transferSchema.fieldNames.sameElements(Array("order_id")))
    val emptyRow =
      emptyOutput.projector.project(new GenericInternalRow(Array[Any](42L)))
    assert(emptyRow.numFields == 0)

    println("READ_SCHEMA_PLAN_TEST_OK")
  }
}
