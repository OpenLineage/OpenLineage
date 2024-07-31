/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.facets.builder;

import io.openlineage.spark.agent.facets.DebugRunFacet;
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

  public DebugRunFacetBuilder(OpenLineageContext openLineageContext) {
    this.delegate = new DebugRunFacetBuilderDelegate(openLineageContext);
    this.openLineageContext = openLineageContext;
  }

  @Override
  public boolean isDefinedAt(Object x) {
    return isEnabled();
  }

  @Override
  protected void build(Object event, BiConsumer<String, ? super DebugRunFacet> consumer) {
    if (isEnabled()) {
      consumer.accept("debug", delegate.buildFacet());
    }
  }

  private boolean isEnabled() {
    return openLineageContext.getOpenLineageConfig().getFacetsConfig().getDebug().isEnabled();
  }
}
