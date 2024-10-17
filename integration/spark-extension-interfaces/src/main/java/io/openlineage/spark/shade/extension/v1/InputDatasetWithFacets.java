/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.spark.shade.extension.v1;

import io.openlineage.client.OpenLineage.DatasetFacetsBuilder;
import io.openlineage.client.OpenLineage.InputDatasetInputFacetsBuilder;

/**
 * Interface representing an input dataset with associated facets (metadata).
 *
 * <p>Classes implementing this interface provide methods to retrieve builders for both dataset
 * facets and input dataset input facets. These facets capture metadata associated with the input
 * dataset.
 */
public interface InputDatasetWithFacets {
  /**
   * Returns the {@link DatasetFacetsBuilder} for building dataset facets.
   *
   * <p>Dataset facets include general metadata associated with the dataset.
   *
   * @return the dataset facets builder
   */
  DatasetFacetsBuilder getDatasetFacetsBuilder();

  /**
   * Returns the {@link InputDatasetInputFacetsBuilder} for building input dataset facets.
   *
   * <p>Input dataset facets include specific metadata related to the input datasets that are being
   * used.
   *
   * @return the input dataset input facets builder
   */
  InputDatasetInputFacetsBuilder getInputFacetsBuilder();
}
