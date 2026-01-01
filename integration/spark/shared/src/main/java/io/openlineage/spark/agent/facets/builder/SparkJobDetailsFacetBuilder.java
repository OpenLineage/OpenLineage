/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.facets.builder;

import io.openlineage.spark.agent.facets.SparkJobDetailsFacet;
import io.openlineage.spark.api.CustomFacetBuilder;
import java.util.function.BiConsumer;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.scheduler.SparkListenerJobStart;

/**
 * {@link CustomFacetBuilder} that adds the {@link SparkJobDetailsFacet} to a run. This facet is
 * generated for every {@link SparkListenerJobStart}.
 */
public class SparkJobDetailsFacetBuilder
    extends CustomFacetBuilder<SparkListenerJobStart, SparkJobDetailsFacet> {

  @Override
  protected void build(
      SparkListenerJobStart event, BiConsumer<String, ? super SparkJobDetailsFacet> consumer) {
    consumer.accept("spark_jobDetails", new SparkJobDetailsFacet(event));
  }

  public SparkJobDetailsFacet buildFacet(SparkListenerEvent event) {
    if (event instanceof SparkListenerJobStart) {
      return new SparkJobDetailsFacet((SparkListenerJobStart) event);
    }
    return null;
  }
}
