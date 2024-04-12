/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.facets.builder;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.Versions;
import io.openlineage.spark.agent.facets.SparkPropertyFacet;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.api.OpenLineageContext;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.function.Consumer;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.scheduler.SparkListenerJobStart;
import org.junit.jupiter.api.Test;

@SuppressWarnings("PMD.AvoidDuplicateLiterals")
class SparkPropertyFacetBuilderTest {

  @Test
  void testBuildDefault() {
    SparkContext sparkContext = mock(SparkContext.class);
    when(sparkContext.getConf())
        .thenReturn(
            new SparkConf()
                .setAppName("SparkPropertyFacetBuilderTest")
                .setMaster("local")
                .set("test.key.1", "test"));
    Consumer<OpenLineage.RunFacet> runFacetConsumer =
        facet -> {
          assertThat(facet).isInstanceOf(SparkPropertyFacet.class);

          assertThat(((SparkPropertyFacet) facet).getProperties())
              .containsOnlyKeys("spark.master", "spark.app.name")
              .containsEntry("spark.master", "local")
              .containsEntry("spark.app.name", "SparkPropertyFacetBuilderTest");
        };

    checkBuild(sparkContext, runFacetConsumer);
  }

  @Test
  void testCustomPropertiesBuild() {
    SparkContext sparkContext = mock(SparkContext.class);
    when(sparkContext.getConf())
        .thenReturn(
            new SparkConf()
                .setAppName("SparkPropertyFacetBuilderTest")
                .setMaster("local")
                .set("test.key.1", "test")
                .set("spark.openlineage.capturedProperties", "test.key.1"));

    Consumer<OpenLineage.RunFacet> runFacetConsumer =
        facet -> {
          assertThat(facet).isInstanceOf(SparkPropertyFacet.class);
          assertThat(((SparkPropertyFacet) facet).getProperties())
              .containsOnlyKeys("test.key.1")
              .containsEntry("test.key.1", "test");
        };
    checkBuild(sparkContext, runFacetConsumer);
  }

  private static void checkBuild(
      SparkContext sparkContext, Consumer<OpenLineage.RunFacet> runFacetConsumer) {
    SparkPropertyFacetBuilder builder =
        new SparkPropertyFacetBuilder(
            OpenLineageContext.builder()
                .sparkContext(sparkContext)
                .openLineage(new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI))
                .meterRegistry(new SimpleMeterRegistry())
                .build());

    Map<String, OpenLineage.RunFacet> runFacetMap = new HashMap<>();
    builder.build(
        new SparkListenerJobStart(1, 1L, ScalaConversionUtils.asScalaSeqEmpty(), new Properties()),
        runFacetMap::put);

    assertThat(runFacetMap).hasEntrySatisfying("spark_properties", runFacetConsumer);
  }
}
