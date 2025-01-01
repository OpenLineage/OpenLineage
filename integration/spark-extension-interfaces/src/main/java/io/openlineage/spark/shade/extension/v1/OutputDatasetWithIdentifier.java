/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.spark.shade.extension.v1;

import io.openlineage.client.OpenLineage.DatasetFacetsBuilder;
import io.openlineage.client.OpenLineage.OutputDatasetOutputFacetsBuilder;
import io.openlineage.client.utils.DatasetIdentifier;
import java.util.Objects;

/**
 * Represents an output dataset associated with an identifier that includes the dataset's namespace
 * and name.
 *
 * <p>This class provides methods to retrieve the dataset's identifier, as well as builders for the
 * dataset's facets and output dataset facets. It implements both {@link OutputDatasetWithFacets}
 * and {@link DatasetWithIdentifier}.
 */
public class OutputDatasetWithIdentifier implements OutputDatasetWithFacets, DatasetWithIdentifier {

  private final DatasetIdentifier datasetIdentifier;
  private final DatasetFacetsBuilder facetsBuilder;
  private final OutputDatasetOutputFacetsBuilder outputFacetsBuilder;

  /**
   * Constructs a new {@code OutputDatasetWithIdentifier}.
   *
   * @param datasetIdentifier the identifier of the dataset, containing its namespace and name
   * @param facetsBuilder a builder for the dataset facets
   * @param outputFacetsBuilder a builder for the output dataset facets
   */
  public OutputDatasetWithIdentifier(
      DatasetIdentifier datasetIdentifier,
      DatasetFacetsBuilder facetsBuilder,
      OutputDatasetOutputFacetsBuilder outputFacetsBuilder) {
    this.datasetIdentifier = datasetIdentifier;
    this.facetsBuilder = facetsBuilder;
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
    return facetsBuilder;
  }

  /**
   * Returns the {@link OutputDatasetOutputFacetsBuilder} for building output dataset facets.
   *
   * <p>Output dataset facets include specific metadata related to the output datasets that are
   * being written.
   *
   * @return the output dataset facets builder
   */
  @Override
  public OutputDatasetOutputFacetsBuilder getOutputFacetsBuilder() {
    return outputFacetsBuilder;
  }

  /**
   * Returns the {@link DatasetIdentifier} that contains the dataset's namespace and name.
   *
   * @return the dataset identifier
   */
  @Override
  public DatasetIdentifier getDatasetIdentifier() {
    return datasetIdentifier;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    OutputDatasetWithIdentifier that = (OutputDatasetWithIdentifier) o;
    return Objects.equals(datasetIdentifier, that.datasetIdentifier)
        && Objects.equals(facetsBuilder, that.facetsBuilder)
        && Objects.equals(outputFacetsBuilder, that.outputFacetsBuilder);
  }

  @Override
  public int hashCode() {
    return Objects.hash(datasetIdentifier, facetsBuilder, outputFacetsBuilder);
  }
}
