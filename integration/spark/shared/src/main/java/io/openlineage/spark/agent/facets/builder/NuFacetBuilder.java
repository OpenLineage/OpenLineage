/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.facets.builder;

import io.openlineage.spark.agent.facets.NuFacet;
import io.openlineage.client.OpenLineage.RunEvent;
import io.openlineage.spark.api.CustomFacetBuilder;
import io.openlineage.spark.api.OpenLineageContext;
import java.util.function.BiConsumer;
import org.apache.spark.scheduler.SparkListenerEvent;


public class NuFacetBuilder
    extends CustomFacetBuilder<SparkListenerEvent, NuFacet> {

  private final OpenLineageContext olContext;

  public NuFacetBuilder(OpenLineageContext olContext) {
    this.olContext = olContext;
  }

  @Override
  protected void build(
      SparkListenerEvent event, BiConsumer<String, ? super NuFacet> consumer) {
    consumer.accept("nu_facets", new NuFacet(olContext));
  }
}
