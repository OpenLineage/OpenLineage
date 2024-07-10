/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.facets.builder;

import static org.assertj.core.api.Assertions.assertThat;

import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.client.OpenLineage.RunFacet;
import io.openlineage.spark.agent.facets.SparkJobDetailsFacet;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.spark.scheduler.SparkListenerJobStart;
import org.junit.jupiter.api.Test;

class NuFacetBuilderTest {
  private static OpenLineageContext openLineageContext = mock(OpenLineageContext.class);

  @Test
  void testBuild() {
    openLineageContext.setJobNurn("nurn:blablabla")
    NuFacetBuilder builder = new NuFacetBuilder(openLineageContext);
    Map<String, RunFacet> runFacetMap = new HashMap<>();
    builder.build(
        new SparkListenerJobStart(1, 1l, ScalaConversionUtils.asScalaSeqEmpty(), Properties properties = new Properties()),
        runFacetMap::put);
    assertThat(runFacetMap)
        .hasEntrySatisfying(
            "nu_facets",
            facet ->
                assertThat(facet)
                    .isInstanceOf(NuFacet.class)
                    .hasFieldOrPropertyWithValue("jobNurn", "nurn:blablabla"));
  }
