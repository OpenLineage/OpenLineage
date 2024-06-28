/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.shaded.spark.extension.v1;

import io.openlineage.client.OpenLineage.DatasetFacetsBuilder;
import io.openlineage.client.OpenLineage.OutputDatasetOutputFacetsBuilder;
import io.openlineage.client.utils.DatasetIdentifier;
import java.util.Objects;

/** Output dataset with an identifier containing namespace and name */
public class OutputDatasetWithIdentifier implements OutputDatasetWithFacets, DatasetWithIdentifier {

  private final DatasetIdentifier datasetIdentifier;
  private final DatasetFacetsBuilder facetsBuilder;
  private final OutputDatasetOutputFacetsBuilder outputFacetsBuilder;

  public OutputDatasetWithIdentifier(
      DatasetIdentifier datasetIdentifier,
      DatasetFacetsBuilder facetsBuilder,
      OutputDatasetOutputFacetsBuilder outputFacetsBuilder) {
    this.datasetIdentifier = datasetIdentifier;
    this.facetsBuilder = facetsBuilder;
    this.outputFacetsBuilder = outputFacetsBuilder;
  }

  @Override
  public DatasetFacetsBuilder getDatasetFacetsBuilder() {
    return facetsBuilder;
  }

  @Override
  public OutputDatasetOutputFacetsBuilder getOutputFacetsBuilder() {
    return outputFacetsBuilder;
  }

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
