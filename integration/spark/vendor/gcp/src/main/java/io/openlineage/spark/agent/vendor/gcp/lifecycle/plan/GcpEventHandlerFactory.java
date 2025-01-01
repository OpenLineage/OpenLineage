/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.vendor.gcp.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.vendor.gcp.facets.builder.GcpJobFacetBuilder;
import io.openlineage.spark.agent.vendor.gcp.facets.builder.GcpRunFacetBuilder;
import io.openlineage.spark.api.CustomFacetBuilder;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.OpenLineageEventHandlerFactory;
import java.util.Collection;
import java.util.Collections;

public class GcpEventHandlerFactory implements OpenLineageEventHandlerFactory {

  @Override
  public Collection<CustomFacetBuilder<?, ? extends OpenLineage.RunFacet>> createRunFacetBuilders(
      OpenLineageContext context) {
    return Collections.singletonList(new GcpRunFacetBuilder(context));
  }

  @Override
  public Collection<CustomFacetBuilder<?, ? extends OpenLineage.JobFacet>> createJobFacetBuilders(
      OpenLineageContext context) {
    return Collections.singletonList(new GcpJobFacetBuilder(context.getSparkContext().get()));
  }
}
