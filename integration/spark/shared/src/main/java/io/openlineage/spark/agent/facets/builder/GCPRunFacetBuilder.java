/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.facets.builder;

import io.openlineage.spark.agent.facets.GCPRunFacet;
import io.openlineage.spark.agent.util.GCPUtils;
import io.openlineage.spark.api.CustomFacetBuilder;
import io.openlineage.spark.api.OpenLineageContext;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiConsumer;
import org.apache.spark.SparkContext;
import org.apache.spark.scheduler.SparkListenerEvent;

/**
 * {@link CustomFacetBuilder} responsible for generating {@link GCPRunFacet} when using OpenLineage
 * on Google Cloud Platform (GCP).
 */
public class GCPRunFacetBuilder extends CustomFacetBuilder<SparkListenerEvent, GCPRunFacet> {

  private final SparkContext sparkContext;
  private final Optional<OpenLineageContext> maybeOLContext;

  public GCPRunFacetBuilder(OpenLineageContext openLineageContext) {
    this.sparkContext = openLineageContext.getSparkContext();
    this.maybeOLContext = Optional.of(openLineageContext);
  }

  public GCPRunFacetBuilder(SparkContext sparkContext) {
    this.sparkContext = sparkContext;
    this.maybeOLContext = Optional.empty();
  }

  @Override
  protected void build(SparkListenerEvent event, BiConsumer<String, ? super GCPRunFacet> consumer) {
    if (GCPUtils.isDataprocRuntime())
      consumer.accept("gcp_dataproc", new GCPRunFacet(getDataprocAttributes()));
  }

  private Map<String, Object> getDataprocAttributes() {
    Map<String, Object> dpProperties = GCPUtils.getDataprocRunFacetMap(sparkContext);
    maybeOLContext
        .flatMap(GCPUtils::getSparkQueryExecutionNodeName)
        .ifPresent(p -> dpProperties.put("spark.query.node.name", p));
    return dpProperties;
  }
}
