package io.openlineage.spark.extension.scala.v1

import io.openlineage.client.utils.DatasetIdentifier

trait ColumnLevelLineageNode {
  def columnLevelLineageInputs(
      context: OpenLineageExtensionContext
  ): List[DatasetFieldLineage] = List()
  def columnLevelLineageOutputs(
      context: OpenLineageExtensionContext
  ): List[DatasetFieldLineage] = List()
  def columnLevelLineageDependencies(
      context: OpenLineageExtensionContext
  ): List[ExpressionDependency] = List()
}

sealed trait DatasetFieldLineage

/** Output field requires only field name and expression identifier. Proper
  * dataset identifier can be extracted from output dataset of the job available
  * when constructing column lineage facet. This is possible as only one field
  * with the same name is allowed in output for Spark.
  *
  * @param field
  */
case class OutputDatasetField(field: String, exprId: OlExprId)
    extends DatasetFieldLineage

sealed trait FieldWithExprId extends DatasetFieldLineage {
  def field: String;
  def exprId: OlExprId;
}

trait DatasetFieldWithIdentifier extends DatasetFieldLineage {
  def datasetIdentifier: DatasetIdentifier;
  def fieldWithExprId: FieldWithExprId
}

case class InputDatasetFieldWithIdentifier(
    datasetIdentifier: DatasetIdentifier,
    field: String,
    exprId: OlExprId
) extends DatasetFieldLineage
    with FieldWithExprId

case class InputDatasetFieldFromDelegate(
    delegate: Object
) extends DatasetFieldLineage

sealed trait ExpressionDependency {
  def outputExprId: OlExprId;
}

/** Class to contain reference to expression nodes in a Spark plan. Input
  * expression (or expressions) have to be extracted from expression object
  */
case class ExpressionDependencyWithDelegate(
    outputExprId: OlExprId,
    expression: Object
) extends ExpressionDependency

case class ExpressionDependencyWithIdentifier(
    outputExprId: OlExprId,
    inputExprIds: List[OlExprId]
) extends ExpressionDependency

/** Class to contain reference to Spark's ExprId without adding dependency to
  * Spark library
  *
  * @see
  *   <a
  *   href="https://github.com/apache/spark/blob/ce5ddad990373636e94071e7cef2f31021add07b/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/namedExpressions.scala#L48">
  *
  * @param exprId
  */
case class OlExprId(exprId: Long)
