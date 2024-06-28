/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.shaded.spark.extension.v1;

import io.openlineage.client.OpenLineage.DatasetFacetsBuilder;
import io.openlineage.client.OpenLineage.InputDatasetInputFacetsBuilder;
import io.openlineage.client.utils.DatasetIdentifier;
import java.util.Objects;

/** Input dataset with an identifier containing namespace and name */
public class InputDatasetWithIdentifier implements InputDatasetWithFacets, DatasetWithIdentifier {

  private final DatasetIdentifier datasetIdentifier;
  private final DatasetFacetsBuilder facetsBuilder;
  private final InputDatasetInputFacetsBuilder inputFacetsBuilder;

  public InputDatasetWithIdentifier(
      DatasetIdentifier datasetIdentifier,
      DatasetFacetsBuilder facetsBuilder,
      InputDatasetInputFacetsBuilder inputFacetsBuilder) {
    this.datasetIdentifier = datasetIdentifier;
    this.facetsBuilder = facetsBuilder;
    this.inputFacetsBuilder = inputFacetsBuilder;
  }

  @Override
  public DatasetFacetsBuilder getDatasetFacetsBuilder() {
    return facetsBuilder;
  }

  @Override
  public InputDatasetInputFacetsBuilder getInputFacetsBuilder() {
    return inputFacetsBuilder;
  }

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
