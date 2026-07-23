package com.itsumma.gpconnector.reader

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.itsumma.gpconnector.SparkSchemaUtil
import org.apache.spark.sql.types.StructType

import java.sql.SQLException

private[gpconnector] final case class ReadSchemaPlan(
    outputSchema: StructType,
    transferSchema: StructType,
    distributionClause: String,
    unresolvedDistributionColumns: Seq[String]) {

  val projector: ReadRowProjector =
    new ReadRowProjector(transferSchema, outputSchema)
}

private[gpconnector] object ReadSchemaPlan {
  def build(
      sourceSchema: StructType,
      outputSchema: StructType,
      distributionClause: String,
      distributionColNames: String): ReadSchemaPlan = {
    val identifiers = parseIdentifiers(distributionColNames)
    if (identifiers.isEmpty) {
      return ReadSchemaPlan(
        outputSchema,
        outputSchema,
        distributionClause,
        Seq.empty)
    }

    val unresolved = identifiers.filter(identifier =>
      resolveField(sourceSchema, identifier).isEmpty)
    if (unresolved.nonEmpty) {
      return ReadSchemaPlan(
        outputSchema,
        outputSchema,
        "distributed randomly",
        unresolved.map(_.raw))
    }

    val outputFields = outputSchema.fields.toSeq
    val hiddenDistributionFields = identifiers.flatMap { identifier =>
      val alreadyOutput = outputFields.exists(field =>
        identifier.matches(field.name))
      if (alreadyOutput) None
      else resolveField(sourceSchema, identifier)
    }
    val transferSchema =
      StructType(outputFields ++ hiddenDistributionFields.distinct)
    ReadSchemaPlan(
      outputSchema,
      transferSchema,
      distributionClause,
      Seq.empty)
  }

  private final case class DistributionIdentifier(
      raw: String,
      name: String,
      quoted: Boolean) {
    def matches(candidate: String): Boolean =
      if (quoted) candidate == name else candidate.equalsIgnoreCase(name)
  }

  private def parseIdentifiers(value: String): Seq[DistributionIdentifier] =
    Option(value).getOrElse("")
      .split(",")
      .map(_.trim)
      .filter(_.nonEmpty)
      .map { token =>
        val quoted =
          token.length >= 2 && token.startsWith("\"") && token.endsWith("\"")
        val name =
          if (quoted) token.substring(1, token.length - 1).replace("\"\"", "\"")
          else token
        DistributionIdentifier(token, name, quoted)
      }
      .toSeq

  private def resolveField(
      schema: StructType,
      identifier: DistributionIdentifier) =
    schema.fields.find(field => identifier.matches(field.name))
}

private[gpconnector] final class ReadRowProjector(
    transferSchema: StructType,
    outputSchema: StructType) extends Serializable {

  private val outputIndexes: Array[Int] = outputSchema.fields.map { outputField =>
    val exactIndex =
      transferSchema.fields.indexWhere(_.name == outputField.name)
    val index =
      if (exactIndex >= 0) exactIndex
      else transferSchema.fields.indexWhere(
        _.name.equalsIgnoreCase(outputField.name))
    require(
      index >= 0,
      s"Output column ${outputField.name} is absent from transfer schema")
    index
  }

  def projectText(
      schemaUtil: SparkSchemaUtil,
      fields: Array[String]): InternalRow = {
    if (transferSchema.length != fields.length) {
      throw new SQLException(
        s"ReadRowProjector: transfer schema.size=${transferSchema.length}, " +
          s"but ${fields.length} data columns received")
    }
    schemaUtil.textToInternalRow(outputSchema, fields, outputIndexes)
  }
}
