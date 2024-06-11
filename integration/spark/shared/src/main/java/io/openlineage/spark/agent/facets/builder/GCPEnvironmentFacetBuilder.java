/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.facets.builder;

import io.openlineage.spark.agent.facets.EnvironmentFacet;
import io.openlineage.spark.agent.util.GCPUtils;
import io.openlineage.spark.api.CustomFacetBuilder;
import io.openlineage.spark.api.OpenLineageContext;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiConsumer;
import org.apache.spark.SparkContext;
import org.apache.spark.scheduler.SparkListenerEvent;

/**
 * {@link CustomFacetBuilder} that generates an {@link EnvironmentFacet} when using OpenLineage on
 * Google Cloud Platform (GCP).
 */
public class GCPEnvironmentFacetBuilder
    extends CustomFacetBuilder<SparkListenerEvent, EnvironmentFacet> {

  private final SparkContext sparkContext;
  private final Optional<OpenLineageContext> openLineageContext;

  public GCPEnvironmentFacetBuilder(OpenLineageContext openLineageContext) {
    this.sparkContext = openLineageContext.getSparkContext();
    this.openLineageContext = Optional.of(openLineageContext);
  }

  public GCPEnvironmentFacetBuilder(SparkContext sparkContext) {
    this.sparkContext = sparkContext;
    this.openLineageContext = Optional.empty();
  }

  @Override
  protected void build(
      SparkListenerEvent event, BiConsumer<String, ? super EnvironmentFacet> consumer) {
    consumer.accept(
        "environment-properties", new EnvironmentFacet(getGCPEnvironmentalAttributes()));
  }

  private Map<String, Object> getGCPEnvironmentalAttributes() {
    Map<String, Object> dbProperties = GCPUtils.getDataprocSpecificFacets(sparkContext);
    openLineageContext
        .flatMap(GCPUtils::getSparkQueryExecutionNodeName)
        .ifPresent(p -> dbProperties.put("spark.query.node.name", p));
    return dbProperties;
  }
}
