/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.spark.shade.extension.v1;

import io.openlineage.client.OpenLineage.DatasetFacetsBuilder;
import io.openlineage.client.OpenLineage.OutputDatasetOutputFacetsBuilder;
import java.util.Objects;

/**
 * Represents an output dataset associated with a node in a LogicalPlan. This class allows for the
 * extraction of metadata (facets) from output datasets.
 *
 * <p>It implements both {@link OutputDatasetWithFacets} and {@link DatasetWithDelegate}, providing
 * methods to retrieve dataset facets, output dataset facets, and the node from which the dataset is
 * extracted.
 */
public class OutputDatasetWithDelegate implements OutputDatasetWithFacets, DatasetWithDelegate {

  private final Object node;
  private final DatasetFacetsBuilder datasetFacetsBuilder;
  private final OutputDatasetOutputFacetsBuilder outputFacetsBuilder;

  /**
   * Constructs a new {@code OutputDatasetWithDelegate}.
   *
   * @param node the node in the LogicalPlan from which the output dataset is extracted
   * @param datasetFacetsBuilder a builder for the dataset facets
   * @param outputFacetsBuilder a builder for the output dataset output facets
   */
  public OutputDatasetWithDelegate(
      Object node,
      DatasetFacetsBuilder datasetFacetsBuilder,
      OutputDatasetOutputFacetsBuilder outputFacetsBuilder) {
    this.node = node;
    this.datasetFacetsBuilder = datasetFacetsBuilder;
    this.outputFacetsBuilder = outputFacetsBuilder;
  }

  /**
   * Returns the {@link DatasetFacetsBuilder} for building dataset facets.
   *
   * <p>Dataset facets include general metadata associated with the dataset.
   *
   * @return the dataset facets builder
   */
  @Override
  public DatasetFacetsBuilder getDatasetFacetsBuilder() {
    return datasetFacetsBuilder;
  }

  /**
   * Returns the {@link OutputDatasetOutputFacetsBuilder} for building output dataset facets.
   *
   * <p>Output dataset facets include specific metadata related to the output datasets.
   *
   * @return the output dataset facets builder
   */
  @Override
  public OutputDatasetOutputFacetsBuilder getOutputFacetsBuilder() {
    return outputFacetsBuilder;
  }

  /**
   * Returns the node in the LogicalPlan from which the output dataset is extracted.
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
