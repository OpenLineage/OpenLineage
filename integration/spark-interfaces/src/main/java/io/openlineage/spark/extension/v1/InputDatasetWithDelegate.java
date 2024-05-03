/**
 * Copyright 2018-2024 contributors to the OpenLineage project SPDX-License-Identifier: Apache-2.0
 */
package io.openlineage.spark.extension.v1;

import io.openlineage.client.OpenLineage.DatasetFacetsBuilder;
import io.openlineage.client.OpenLineage.InputDatasetInputFacetsBuilder;
import java.util.Objects;

/** Input dataset with a node in LogicalPlan where a input dataset shall be extracted from */
public class InputDatasetWithDelegate implements InputDatasetWithFacets, DatasetWithDelegate {

  private final Object node;
  private final DatasetFacetsBuilder datasetFacetsBuilder;
  private final InputDatasetInputFacetsBuilder inputFacetsBuilder;

  /**
   * @param node
   * @param datasetFacetsBuilder
   * @param inputFacetsBuilder
   */
  public InputDatasetWithDelegate(
      Object node,
      DatasetFacetsBuilder datasetFacetsBuilder,
      InputDatasetInputFacetsBuilder inputFacetsBuilder) {
    this.node = node;
    this.datasetFacetsBuilder = datasetFacetsBuilder;
    this.inputFacetsBuilder = inputFacetsBuilder;
  }

  @Override
  public DatasetFacetsBuilder getDatasetFacetBuilder() {
    return datasetFacetsBuilder;
  }

  @Override
  public InputDatasetInputFacetsBuilder getInputFacetsBuilder() {
    return inputFacetsBuilder;
  }

  @Override
  public Object getNode() {
    return node;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    InputDatasetWithDelegate that = (InputDatasetWithDelegate) o;
    return Objects.equals(node, that.node)
        && Objects.equals(datasetFacetsBuilder, that.datasetFacetsBuilder)
        && Objects.equals(inputFacetsBuilder, that.inputFacetsBuilder);
  }

  @Override
  public int hashCode() {
    return Objects.hash(node, datasetFacetsBuilder, inputFacetsBuilder);
  }
}
