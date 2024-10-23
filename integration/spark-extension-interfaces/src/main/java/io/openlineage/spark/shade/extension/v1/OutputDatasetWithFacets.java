/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.spark.shade.extension.v1;

import io.openlineage.client.OpenLineage.DatasetFacetsBuilder;
import io.openlineage.client.OpenLineage.OutputDatasetOutputFacetsBuilder;

/**
 * Interface representing an output dataset with associated facets (metadata).
 *
 * <p>Classes implementing this interface provide methods to retrieve builders for both dataset
 * facets and output dataset facets. These facets capture metadata associated with the output
 * dataset.
 */
public interface OutputDatasetWithFacets {
  /**
   * Returns the {@link DatasetFacetsBuilder} for building dataset facets.
   *
   * <p>Dataset facets include general metadata associated with the dataset.
   *
   * @return the dataset facets builder
   */
  DatasetFacetsBuilder getDatasetFacetsBuilder();

  /**
   * Returns the {@link OutputDatasetOutputFacetsBuilder} for building output dataset facets.
   *
   * <p>Output dataset facets include specific metadata related to the output datasets being
   * written.
   *
   * @return the output dataset facets builder
   */
  OutputDatasetOutputFacetsBuilder getOutputFacetsBuilder();
}
