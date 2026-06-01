/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.api;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.dataset.DatasetCompositeFacetsBuilder;
import io.openlineage.spark.agent.util.DatasetVersionUtils;

public class SparkOutputDatasetCompositeFacetsBuilder
    extends SparkDatasetCompositeFacetsBuilder<OpenLineage.OutputDataset> {

  public SparkOutputDatasetCompositeFacetsBuilder(OpenLineageContext context) {
    super(context);
  }

  public SparkOutputDatasetCompositeFacetsBuilder(
      OpenLineageContext context, DatasetCompositeFacetsBuilder inner) {
    super(context, inner);
  }

  /** Overrides to also trigger vendor-specific output facet builders (e.g. Iceberg). */
  @Override
  public SparkDatasetCompositeFacetsBuilder<OpenLineage.OutputDataset> version(String version) {
    DatasetVersionUtils.buildVersionOutputFacets(context, inner, version);
    return this;
  }

  @Override
  public OpenLineage.OutputDataset build() {
    return context
        .getOpenLineage()
        .newOutputDatasetBuilder()
        .namespace(namespaceResolver.resolve(namespace))
        .name(name)
        .outputFacets(inner.getOutputFacets().build())
        .facets(inner.getFacets().build())
        .build();
  }

  @Override
  public SparkDatasetCompositeFacetsBuilder<OpenLineage.OutputDataset> fromBuilder(
      OpenLineage.DatasetFacetsBuilder facetsBuilder) {
    inner.setFacets(facetsBuilder);
    return this;
  }
}
