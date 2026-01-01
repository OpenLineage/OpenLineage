/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.facets.builder;

import io.openlineage.spark.agent.facets.SparkApplicationDetailsFacet;
import io.openlineage.spark.api.CustomFacetBuilder;
import io.openlineage.spark.api.OpenLineageContext;
import java.util.Optional;
import java.util.function.BiConsumer;
import org.apache.spark.SparkContext;
import org.apache.spark.scheduler.SparkListenerApplicationStart;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.scheduler.SparkListenerJobStart;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart;

/**
 * {@link CustomFacetBuilder} that adds the {@link SparkApplicationDetailsFacet} to a run. This
 * facet is generated for every {@link SparkListenerEvent}.
 */
public class SparkApplicationDetailsFacetBuilder
    extends CustomFacetBuilder<SparkListenerEvent, SparkApplicationDetailsFacet> {

  private Optional<SparkContext> sparkContext;

  public SparkApplicationDetailsFacetBuilder(OpenLineageContext context) {
    this.sparkContext = context.getSparkContext();
  }

  @Override
  protected void build(
      SparkListenerEvent event, BiConsumer<String, ? super SparkApplicationDetailsFacet> consumer) {
    if (!(event instanceof SparkListenerApplicationStart
        || event instanceof SparkListenerJobStart
        || event instanceof SparkListenerSQLExecutionStart)) {
      return;
    }
    sparkContext.ifPresent(
        context ->
            consumer.accept("spark_applicationDetails", new SparkApplicationDetailsFacet(context)));
  }
}
