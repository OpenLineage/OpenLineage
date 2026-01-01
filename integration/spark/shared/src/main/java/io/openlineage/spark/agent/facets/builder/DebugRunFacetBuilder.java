/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.facets.builder;

import io.openlineage.spark.agent.facets.DebugRunFacet;
import io.openlineage.spark.agent.util.FacetUtils;
import io.openlineage.spark.api.CustomFacetBuilder;
import io.openlineage.spark.api.OpenLineageContext;
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
    return !FacetUtils.isFacetDisabled(openLineageContext, "debug");
  }

  @Override
  protected void build(Object event, BiConsumer<String, ? super DebugRunFacet> consumer) {
    consumer.accept("debug", delegate.buildFacet());
  }
}
