/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.api;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.dataset.DatasetCompositeFacetsBuilder;

public class SparkInputDatasetBuilder extends SparkDatasetBuilder<OpenLineage.InputDataset> {

  public SparkInputDatasetBuilder(OpenLineageContext context) {
    super(context);
  }

  public SparkInputDatasetBuilder(OpenLineageContext context, DatasetCompositeFacetsBuilder inner) {
    super(context, inner);
  }

  @Override
  public OpenLineage.InputDataset build() {
    return context
        .getOpenLineage()
        .newInputDatasetBuilder()
        .namespace(namespaceResolver.resolve(namespace))
        .name(name)
        .inputFacets(inner.getInputFacets().build())
        .facets(inner.getFacets().build())
        .build();
  }

  @Override
  public SparkDatasetBuilder<OpenLineage.InputDataset> fromBuilder(
      OpenLineage.DatasetFacetsBuilder facetsBuilder) {
    inner.setFacets(facetsBuilder);
    return this;
  }
}
