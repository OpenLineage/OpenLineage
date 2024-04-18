/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.facets.builder;

import io.openlineage.spark.agent.facets.DebugRunFacet;
import io.openlineage.spark.api.CustomFacetBuilder;
import io.openlineage.spark.api.OpenLineageContext;
import java.util.Optional;
import java.util.function.BiConsumer;
import org.apache.spark.scheduler.SparkListenerEvent;

/**
 * {@link CustomFacetBuilder} that adds the {@link DebugRunFacet} to a run. This facet is generated
 * for every {@link SparkListenerEvent}.
 */
public class DebugRunFacetBuilder extends CustomFacetBuilder<Object, DebugRunFacet> {

  private final DebugRunFacetBuilderDelegate delegate;
  private final OpenLineageContext openLineageContext;

  public DebugRunFacetBuilder(OpenLineageContext openLineageContext1) {
    this.openLineageContext = openLineageContext1;
    this.delegate = new DebugRunFacetBuilderDelegate(openLineageContext);
  }

  @Override
  public boolean isDefinedAt(Object x) {
    return Optional.of(openLineageContext.getSparkContext())
        .map(sc -> sc.conf())
        .map(conf -> conf.get("spark.openlineage.debugFacet", "disabled"))
        .map(value -> "enabled".equalsIgnoreCase(value))
        .orElse(false);
  }

  @Override
  protected void build(Object event, BiConsumer<String, ? super DebugRunFacet> consumer) {
    consumer.accept("debug", delegate.buildFacet());
  }
}
