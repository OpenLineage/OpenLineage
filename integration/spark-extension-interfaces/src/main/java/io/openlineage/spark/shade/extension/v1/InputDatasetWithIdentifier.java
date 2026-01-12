/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.spark.shade.extension.v1;

import io.openlineage.client.OpenLineage.DatasetFacetsBuilder;
import io.openlineage.client.OpenLineage.InputDatasetInputFacetsBuilder;
import io.openlineage.client.utils.DatasetIdentifier;
import java.util.Objects;

/**
 * Represents an input dataset with an associated {@link DatasetIdentifier} containing the dataset's
 * namespace and name.
 *
 * <p>This class provides methods to retrieve the dataset's identifier, as well as builders for the
 * dataset's facets and input dataset input facets. It implements both {@link
 * InputDatasetWithFacets} and {@link DatasetWithIdentifier}.
 */
public class InputDatasetWithIdentifier implements InputDatasetWithFacets, DatasetWithIdentifier {

  private final DatasetIdentifier datasetIdentifier;
  private final DatasetFacetsBuilder facetsBuilder;
  private final InputDatasetInputFacetsBuilder inputFacetsBuilder;

  /**
   * Constructs a new {@code InputDatasetWithIdentifier}.
   *
   * @param datasetIdentifier the identifier of the dataset, containing its namespace and name
   * @param facetsBuilder a builder for the dataset facets
   * @param inputFacetsBuilder a builder for the input dataset input facets
   */
  public InputDatasetWithIdentifier(
      DatasetIdentifier datasetIdentifier,
      DatasetFacetsBuilder facetsBuilder,
      InputDatasetInputFacetsBuilder inputFacetsBuilder) {
    this.datasetIdentifier = datasetIdentifier;
    this.facetsBuilder = facetsBuilder;
    this.inputFacetsBuilder = inputFacetsBuilder;
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
   * Returns the {@link InputDatasetInputFacetsBuilder} for building input dataset input facets.
   *
   * <p>Input dataset facets include specific metadata related to the input datasets that are being
   * used like data quality metrics.
   *
   * @return the input dataset input facets builder
   */
  @Override
  public InputDatasetInputFacetsBuilder getInputFacetsBuilder() {
    return inputFacetsBuilder;
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
    InputDatasetWithIdentifier that = (InputDatasetWithIdentifier) o;
    return Objects.equals(datasetIdentifier, that.datasetIdentifier)
        && Objects.equals(facetsBuilder, that.facetsBuilder)
        && Objects.equals(inputFacetsBuilder, that.inputFacetsBuilder);
  }

  @Override
  public int hashCode() {
    return Objects.hash(datasetIdentifier, facetsBuilder, inputFacetsBuilder);
  }
}
