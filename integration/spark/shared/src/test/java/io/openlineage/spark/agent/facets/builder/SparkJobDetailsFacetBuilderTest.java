/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.facets.builder;

import static org.assertj.core.api.Assertions.assertThat;

import io.openlineage.client.OpenLineage.RunFacet;
import io.openlineage.spark.agent.facets.SparkJobDetailsFacet;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.spark.scheduler.SparkListenerJobStart;
import org.junit.jupiter.api.Test;

class SparkJobDetailsFacetBuilderTest {
  @Test
  void testIsDefinedForSparkListenerEvents() {
    SparkJobDetailsFacetBuilder builder = new SparkJobDetailsFacetBuilder();

    assertThat(
            builder.isDefinedAt(
                new SparkListenerJobStart(
                    1, 1L, ScalaConversionUtils.asScalaSeqEmpty(), new Properties())))
        .isTrue();
  }

  @Test
  void testBuild() {
    SparkJobDetailsFacetBuilder builder = new SparkJobDetailsFacetBuilder();

    Map<String, RunFacet> runFacetMap = new HashMap<>();
    int jobId = 1;
    Properties properties = new Properties();
    properties.setProperty("spark.jobGroup.id", "group");
    properties.setProperty("spark.job.description", "description");
    properties.setProperty("callSite.short", "collect at file:2");
    builder.build(
        new SparkListenerJobStart(jobId, 1l, ScalaConversionUtils.asScalaSeqEmpty(), properties),
        runFacetMap::put);
    assertThat(runFacetMap)
        .hasEntrySatisfying(
            "spark_jobDetails",
            facet ->
                assertThat(facet)
                    .isInstanceOf(SparkJobDetailsFacet.class)
                    .hasFieldOrPropertyWithValue("jobId", jobId)
                    .hasFieldOrPropertyWithValue("jobDescription", "description")
                    .hasFieldOrPropertyWithValue("jobGroup", "group")
                    .hasFieldOrPropertyWithValue("jobCallSite", "collect at file:2"));
  }

  @Test
  void testBuildMinimal() {
    SparkJobDetailsFacetBuilder builder = new SparkJobDetailsFacetBuilder();

    Map<String, RunFacet> runFacetMap = new HashMap<>();
    int jobId = 1;
    builder.build(
        new SparkListenerJobStart(
            jobId, 1L, ScalaConversionUtils.asScalaSeqEmpty(), new Properties()),
        runFacetMap::put);
    assertThat(runFacetMap)
        .hasEntrySatisfying(
            "spark_jobDetails",
            facet ->
                assertThat(facet)
                    .isInstanceOf(SparkJobDetailsFacet.class)
                    .hasFieldOrPropertyWithValue("jobId", jobId)
                    .hasFieldOrPropertyWithValue("jobDescription", null)
                    .hasFieldOrPropertyWithValue("jobGroup", null)
                    .hasFieldOrPropertyWithValue("jobCallSite", null));
  }
}
