/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.util;

import io.openlineage.client.dataset.DatasetCompositeFacetsBuilder;
import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.spark.api.OpenLineageContext;

/**
 * Adds the facets that follow from a dataset's identity: its final name, namespace and symlinks.
 */
public final class DatasetIdentityFacetUtils {
  private DatasetIdentityFacetUtils() {}

  /**
   * Adds every facet derived from the dataset's identity, such as {@code fabric_onelake} for
   * datasets stored in Microsoft Fabric's OneLake.
   */
  public static void buildIdentityFacets(
      OpenLineageContext context,
      DatasetCompositeFacetsBuilder facetsBuilder,
      DatasetIdentifier identifier) {
    OneLakeFacetUtils.facetFor(context, identifier)
        .ifPresent(facet -> facetsBuilder.getFacets().fabric_onelake(facet));
  }
}
