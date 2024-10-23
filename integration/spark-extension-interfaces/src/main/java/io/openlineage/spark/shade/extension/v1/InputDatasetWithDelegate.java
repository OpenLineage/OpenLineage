/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.spark.shade.extension.v1;

import io.openlineage.client.OpenLineage.DatasetFacetsBuilder;
import io.openlineage.client.OpenLineage.InputDatasetInputFacetsBuilder;
import java.util.Objects;

/**
 * Represents an input dataset associated with a node in a LogicalPlan. This class allows the
 * extraction of facets (metadata) from the input dataset.
 *
 * <p>It implements both {@link InputDatasetWithFacets} and {@link DatasetWithDelegate}, providing
 * methods to retrieve dataset facets and the node from which the dataset is extracted.
 */
public class InputDatasetWithDelegate implements InputDatasetWithFacets, DatasetWithDelegate {

  private final Object node;
  private final DatasetFacetsBuilder datasetFacetsBuilder;
  private final InputDatasetInputFacetsBuilder inputFacetsBuilder;

  /**
   * Constructs a new {@code InputDatasetWithDelegate}.
   *
   * @param node the node in the LogicalPlan from which the input dataset is extracted
   * @param datasetFacetsBuilder a builder for the dataset facets
   * @param inputFacetsBuilder a builder for the input dataset input facets
   */
  public InputDatasetWithDelegate(
      Object node,
      DatasetFacetsBuilder datasetFacetsBuilder,
      InputDatasetInputFacetsBuilder inputFacetsBuilder) {
    this.node = node;
    this.datasetFacetsBuilder = datasetFacetsBuilder;
    this.inputFacetsBuilder = inputFacetsBuilder;
  }

  /**
   * Returns the {@link DatasetFacetsBuilder} for building dataset facets.
   *
   * @return the dataset facets builder
   */
  @Override
  public DatasetFacetsBuilder getDatasetFacetsBuilder() {
    return datasetFacetsBuilder;
  }

  /**
   * Returns the {@link InputDatasetInputFacetsBuilder} for building input dataset facets.
   *
   * @return the input dataset input facets builder
   */
  @Override
  public InputDatasetInputFacetsBuilder getInputFacetsBuilder() {
    return inputFacetsBuilder;
  }

  /**
   * Returns the node in the LogicalPlan from which the input dataset is extracted.
   *
   * @return the LogicalPlan node
   */
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
