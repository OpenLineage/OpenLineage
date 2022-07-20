/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.util;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.api.OpenLineageContext;

public class DatasetFacetsUtils {

  public static OpenLineage.DatasetFacetsBuilder copyToBuilder(
      OpenLineageContext context, OpenLineage.DatasetFacets facets) {
    return context
        .getOpenLineage()
        .newDatasetFacetsBuilder()
        .columnLineage(facets.getColumnLineage())
        .storage(facets.getStorage())
        .schema(facets.getSchema())
        .documentation(facets.getDocumentation())
        .dataSource(facets.getDataSource())
        .symlinks(facets.getSymlinks())
        .version(facets.getVersion())
        .lifecycleStateChange(facets.getLifecycleStateChange());
  }
}
