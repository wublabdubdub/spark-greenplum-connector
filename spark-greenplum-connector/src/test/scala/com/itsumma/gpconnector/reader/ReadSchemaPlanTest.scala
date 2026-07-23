package com.itsumma.gpconnector.reader

import org.apache.spark.sql.itsumma.gpconnector.SparkSchemaUtil
import org.apache.spark.sql.types._

import java.sql.SQLException

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

    val schemaUtil = SparkSchemaUtil("Asia/Shanghai")
    val projected =
      plan.projector.projectText(schemaUtil, Array("12.34", "42"))
    assert(projected.numFields == 1)
    assert(projected.getDecimal(0, 10, 2).toString == "12.34")

    val reorderedOutput = StructType(Seq(
      source("CaseSensitiveKey"),
      source("amount")))
    val reorderedTransfer = StructType(Seq(
      source("amount"),
      source("CaseSensitiveKey"),
      source("order_id")))
    val reorderedProjector =
      new ReadRowProjector(reorderedTransfer, reorderedOutput)
    val reorderedRow = reorderedProjector.projectText(
      schemaUtil,
      Array("12.34", "hello", "42"))
    assert(reorderedRow.getUTF8String(0).toString == "hello")
    assert(reorderedRow.getDecimal(1, 10, 2).toString == "12.34")

    val nullRow =
      plan.projector.projectText(schemaUtil, Array("NULL", "42"))
    assert(nullRow.isNullAt(0))

    val wrongFieldCount = expectSqlFailure {
      plan.projector.projectText(schemaUtil, Array("12.34"))
    }
    assert(wrongFieldCount.getMessage ==
      "ReadRowProjector: transfer schema.size=2, " +
        "but 1 data columns received")
    val legacyFieldCount = expectSqlFailure {
      schemaUtil.textToInternalRow(
        amountOnly,
        Array("12.34", "unexpected"))
    }
    assert(legacyFieldCount.getMessage ==
      "textToInternalRow: schema.size=1, " +
        "but 2 data columns received")

    val emptyOutput = ReadSchemaPlan.build(
      source,
      new StructType(),
      "distributed by (order_id)",
      "order_id")
    assert(emptyOutput.transferSchema.fieldNames.sameElements(Array("order_id")))
    val emptyRow = emptyOutput.projector.projectText(
      schemaUtil,
      Array("42"))
    assert(emptyRow.numFields == 0)

    println("READ_SCHEMA_PLAN_TEST_OK")
  }

  private def expectSqlFailure(body: => Any): SQLException = {
    try {
      body
      throw new AssertionError("Expected SQLException")
    } catch {
      case failure: SQLException => failure
    }
  }
}
