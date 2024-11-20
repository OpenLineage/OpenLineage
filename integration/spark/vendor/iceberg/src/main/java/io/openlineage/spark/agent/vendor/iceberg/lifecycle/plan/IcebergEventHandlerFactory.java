/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.vendor.iceberg.lifecycle.plan;

import io.openlineage.client.OpenLineage.InputDatasetFacet;
import io.openlineage.spark.api.CustomFacetBuilder;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.OpenLineageEventHandlerFactory;
import java.util.Collection;
import java.util.Collections;

public class IcebergEventHandlerFactory implements OpenLineageEventHandlerFactory {

  @Override
  public Collection<CustomFacetBuilder<?, ? extends InputDatasetFacet>>
      createInputDatasetFacetBuilders(OpenLineageContext context) {
    return Collections.singleton(
        (CustomFacetBuilder<?, ? extends InputDatasetFacet>)
            new IcebergInputStatisticsInputDatasetFacetBuilder(context));
  }
}
