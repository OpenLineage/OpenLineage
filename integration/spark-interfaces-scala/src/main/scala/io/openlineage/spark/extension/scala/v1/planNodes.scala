/** Copyright 2018-2024 contributors to the OpenLineage project
  * SPDX-License-Identifier: Apache-2.0
  */
package io.openlineage.spark.extension.scala.v1

import io.openlineage.client.OpenLineage.{
  DatasetFacetsBuilder,
  InputDatasetInputFacetsBuilder,
  OutputDatasetOutputFacetsBuilder
}
import io.openlineage.client.utils.DatasetIdentifier;

/** Trait to be implemented for LogicalPlan nodes to extract lineage information
  * about input datasets.
  */
trait InputLineageNode {

  /** Gets input dataset read by this LogicalPlans node
    */
  def getInputs(
      context: OpenLineageExtensionContext
  ): List[InputDatasetWithFacets]
}

/** Trait to be implemented for LogicalPlan nodes to extract lineage information
  * about output datasets.
  */
trait OutputLineageNode {

  /** Gets output dataset read by this LogicalPlans node
    */
  def getOutputs(
      context: OpenLineageExtensionContext
  ): List[OutputDatasetWithFacets]
}

sealed trait InputDatasetWithFacets {
  def facetsBuilder: DatasetFacetsBuilder
  def inputFacetsBuilder: InputDatasetInputFacetsBuilder
}

sealed trait OutputDatasetWithFacets {
  def facetsBuilder: DatasetFacetsBuilder
  def outputFacetsBuilder: OutputDatasetOutputFacetsBuilder
}

/** Dataset with an identifier containing namespace and name
  *
  * @param datasetIdentifier
  */
sealed trait DatasetWithIdentifier {
  def datasetIdentifier: DatasetIdentifier
}

/** Dataset with a node in LogicalPlan where a input dataset shall be extracted
  * from
  *
  * @param node
  */
sealed trait DatasetWithDelegate {
  def node: Object
}

/** Input dataset with an identifier containing namespace and name
  *
  * @param datasetIdentifier
  * @param facetsBuilder
  * @param inputFacetsBuilder
  */
case class InputDatasetWithIdentifier(
    datasetIdentifier: DatasetIdentifier,
    facetsBuilder: DatasetFacetsBuilder,
    inputFacetsBuilder: InputDatasetInputFacetsBuilder
) extends InputDatasetWithFacets
    with DatasetWithIdentifier

/** Input dataset with a node in LogicalPlan where a input dataset shall be
  * extracted from
  *
  * @param node
  * @param facetsBuilder
  * @param inputFacetsBuilder
  */
case class InputDatasetWithDelegate(
    node: Object,
    facetsBuilder: DatasetFacetsBuilder,
    inputFacetsBuilder: InputDatasetInputFacetsBuilder
) extends InputDatasetWithFacets
    with DatasetWithDelegate

/** Output dataset with an identifier containing namespace and name
  *
  * @param datasetIdentifier
  * @param facetsBuilder
  * @param outputFacetsBuilder
  */
case class OutputDatasetWithIdentifier(
    datasetIdentifier: DatasetIdentifier,
    facetsBuilder: DatasetFacetsBuilder,
    outputFacetsBuilder: OutputDatasetOutputFacetsBuilder
) extends OutputDatasetWithFacets
    with DatasetWithIdentifier

/** Output dataset with a node in LogicalPlan where a input dataset shall be
  * extracted from
  *
  * @param node
  * @param facetsBuilder
  * @param outputFacetsBuilder
  */
case class OutputDatasetWithDelegate(
    node: Object,
    facetsBuilder: DatasetFacetsBuilder,
    outputFacetsBuilder: OutputDatasetOutputFacetsBuilder
) extends OutputDatasetWithFacets
    with DatasetWithDelegate
