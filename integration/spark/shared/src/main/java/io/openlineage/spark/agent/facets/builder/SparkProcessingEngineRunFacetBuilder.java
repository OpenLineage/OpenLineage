/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.facets.builder;

import io.openlineage.client.OpenLineage.ProcessingEngineRunFacet;
import io.openlineage.client.OpenLineage.RunEvent;
import io.openlineage.spark.api.CustomFacetBuilder;
import io.openlineage.spark.api.OpenLineageContext;
import java.util.function.BiConsumer;
import org.apache.spark.scheduler.SparkListenerEvent;

/**
 * {@link CustomFacetBuilder} that adds the {@link ProcessingEngineRunFacet} to a run. This facet is
 * generated for every {@link SparkListenerEvent}.
 *
 * <p>This class is a hacky way of solving this problem, because it wraps the creation of the {@link
 * ProcessingEngineRunFacet} behind the {@link CustomFacetBuilder} interface. The actual creation is
 * done by the delegate, namely: {@link SparkProcessingEngineRunFacetBuilderDelegate}. Why do we
 * delegate to that? To prevent code duplication when code paths that do not use the {@link
 * CustomFacetBuilder} functionality need to add the aforementioned facet to the {@link RunEvent},
 * for example: {@code RddExecutionContext} located in the "app" subproject.
 */
public class SparkProcessingEngineRunFacetBuilder
    extends CustomFacetBuilder<SparkListenerEvent, ProcessingEngineRunFacet> {

  private final SparkProcessingEngineRunFacetBuilderDelegate delegate;

  public SparkProcessingEngineRunFacetBuilder(OpenLineageContext openLineageContext) {
    this.delegate =
        new SparkProcessingEngineRunFacetBuilderDelegate(
            openLineageContext.getOpenLineage(), openLineageContext.getSparkContext());
  }

  @Override
  protected void build(
      SparkListenerEvent event, BiConsumer<String, ? super ProcessingEngineRunFacet> consumer) {
    consumer.accept("processing_engine", delegate.buildFacet());
  }
}
