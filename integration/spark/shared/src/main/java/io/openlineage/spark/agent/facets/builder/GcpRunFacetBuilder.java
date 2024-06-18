/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.facets.builder;

import io.openlineage.client.OpenLineage.RunFacet;
import io.openlineage.spark.agent.facets.GcpDataprocRunFacet;
import io.openlineage.spark.agent.util.GCPUtils;
import io.openlineage.spark.api.CustomFacetBuilder;
import io.openlineage.spark.api.OpenLineageContext;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiConsumer;
import org.apache.spark.SparkContext;
import org.apache.spark.scheduler.SparkListenerEvent;

/**
 * {@link CustomFacetBuilder} responsible for generating GCP-specific run facets when using
 * OpenLineage on Google Cloud Platform (GCP).
 */
public class GCPRunFacetBuilder extends CustomFacetBuilder<SparkListenerEvent, RunFacet> {

  private final SparkContext sparkContext;
  private final Optional<OpenLineageContext> maybeOLContext;

  public GCPRunFacetBuilder(OpenLineageContext openLineageContext) {
    this.sparkContext = openLineageContext.getSparkContext().get();
    this.maybeOLContext = Optional.of(openLineageContext);
  }

  public GCPRunFacetBuilder(SparkContext sparkContext) {
    this.sparkContext = sparkContext;
    this.maybeOLContext = Optional.empty();
  }

  @Override
  protected void build(SparkListenerEvent event, BiConsumer<String, ? super RunFacet> consumer) {
    if (GCPUtils.isDataprocRuntime())
      consumer.accept("gcp_dataproc", new GcpDataprocRunFacet(getDataprocAttributes()));
  }

  private Map<String, Object> getDataprocAttributes() {
    Map<String, Object> dpProperties = GCPUtils.getDataprocRunFacetMap(sparkContext);
    maybeOLContext
        .flatMap(GCPUtils::getSparkQueryExecutionNodeName)
        .ifPresent(p -> dpProperties.put("queryNodeName", p));
    return dpProperties;
  }
}
