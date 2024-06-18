/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.facets.builder;

import io.openlineage.client.OpenLineage.JobFacet;
import io.openlineage.spark.agent.facets.GCPCommonJobFacet;
import io.openlineage.spark.agent.util.GCPUtils;
import io.openlineage.spark.api.CustomFacetBuilder;
import io.openlineage.spark.api.OpenLineageContext;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;
import org.apache.spark.SparkContext;
import org.apache.spark.scheduler.SparkListenerEvent;

/**
 * {@link CustomFacetBuilder} responsible for generating GCP-specific job facets when using
 * OpenLineage on Google Cloud Platform (GCP).
 */
public class GCPJobFacetBuilder extends CustomFacetBuilder<SparkListenerEvent, JobFacet> {

  private final SparkContext sparkContext;

  public GCPJobFacetBuilder(OpenLineageContext openLineageContext) {
    this.sparkContext = openLineageContext.getSparkContext().get();
  }

  public GCPJobFacetBuilder(SparkContext sparkContext) {
    this.sparkContext = sparkContext;
  }

  @Override
  protected void build(SparkListenerEvent event, BiConsumer<String, ? super JobFacet> consumer) {
    consumer.accept("gcp_common", new GCPCommonJobFacet(getCommonAttributes()));
  }

  private Map<String, Object> getCommonAttributes() {

    Map<String, Object> commonProperties = new HashMap<>();
    commonProperties.put("origin", GCPUtils.getOriginFacetMap(sparkContext));
    return commonProperties;
  }
}
