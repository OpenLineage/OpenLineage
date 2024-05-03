/**
 * Copyright 2018-2024 contributors to the OpenLineage project SPDX-License-Identifier: Apache-2.0
 */
package io.openlineage.spark.extension.v1;

import io.openlineage.client.OpenLineage.DatasetFacetsBuilder;
import io.openlineage.client.OpenLineage.OutputDatasetOutputFacetsBuilder;
import java.util.Objects;

/** Output dataset with a node in LogicalPlan where a input dataset shall be extracted from */
public class OutputDatasetWithDelegate implements OutputDatasetWithFacets, DatasetWithDelegate {

  private final Object node;
  private final DatasetFacetsBuilder datasetFacetsBuilder;
  private final OutputDatasetOutputFacetsBuilder outputFacetsBuilder;

  /**
   * @param node
   * @param datasetFacetsBuilder
   * @param outputFacetsBuilder
   */
  public OutputDatasetWithDelegate(
      Object node,
      DatasetFacetsBuilder datasetFacetsBuilder,
      OutputDatasetOutputFacetsBuilder outputFacetsBuilder) {
    this.node = node;
    this.datasetFacetsBuilder = datasetFacetsBuilder;
    this.outputFacetsBuilder = outputFacetsBuilder;
  }

  @Override
  public DatasetFacetsBuilder getDatasetFacetBuilder() {
    return datasetFacetsBuilder;
  }

  @Override
  public OutputDatasetOutputFacetsBuilder getOutputFacetsBuilder() {
    return outputFacetsBuilder;
  }

  @Override
  public Object getNode() {
    return node;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    OutputDatasetWithDelegate that = (OutputDatasetWithDelegate) o;
    return Objects.equals(node, that.node)
        && Objects.equals(datasetFacetsBuilder, that.datasetFacetsBuilder)
        && Objects.equals(outputFacetsBuilder, that.outputFacetsBuilder);
  }

  @Override
  public int hashCode() {
    return Objects.hash(node, datasetFacetsBuilder, outputFacetsBuilder);
  }
}
