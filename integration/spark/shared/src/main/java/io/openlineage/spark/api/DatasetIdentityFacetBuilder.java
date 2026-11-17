/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.api;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.utils.DatasetIdentifier;

/**
 * Lets a vendor add facets to a dataset based on its name, namespace and symlinks.
 *
 * <p>{@link SparkDatasetBuilder} calls these for every dataset it builds, whichever plan node or
 * connector produced it. A vendor returns builders from {@link
 * OpenLineageEventHandlerFactory#createDatasetIdentityFacetBuilders}.
 */
@FunctionalInterface
public interface DatasetIdentityFacetBuilder {

  /**
   * Adds facets to the dataset.
   *
   * @param identifier the dataset's name and namespace, and the symlinks from its {@code symlinks}
   *     facet
   * @param facets the dataset's facets
   */
  void build(DatasetIdentifier identifier, OpenLineage.DatasetFacetsBuilder facets);
}
