/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.util;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.api.OpenLineageContext;

public class DatasetFacetsUtils {

  /**
   * Returns a builder pre-filled with all facets of {@code facets}, including custom ones, so a
   * caller can replace some of them without dropping the rest.
   */
  public static OpenLineage.DatasetFacetsBuilder copyToBuilder(
      OpenLineageContext context, OpenLineage.DatasetFacets facets) {
    OpenLineage.DatasetFacetsBuilder builder = context.getOpenLineage().newDatasetFacetsBuilder();
    if (facets == null) {
      return builder;
    }
    builder
        .hierarchy(facets.getHierarchy())
        .dataSource(facets.getDataSource())
        .version(facets.getVersion())
        .datasetType(facets.getDatasetType())
        .storage(facets.getStorage())
        .columnLineage(facets.getColumnLineage())
        .lifecycleStateChange(facets.getLifecycleStateChange())
        .dataQualityMetrics(facets.getDataQualityMetrics())
        .tags(facets.getTags())
        .documentation(facets.getDocumentation())
        .schema(facets.getSchema())
        .ownership(facets.getOwnership())
        .catalog(facets.getCatalog())
        .symlinks(facets.getSymlinks())
        .lineage(facets.getLineage());
    if (facets.getAdditionalProperties() != null) {
      facets.getAdditionalProperties().forEach(builder::put);
    }
    return builder;
  }
}
