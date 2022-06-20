/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.facets.builder;

import static org.assertj.core.api.Assertions.assertThat;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.RunFacet;
import io.openlineage.spark.agent.Versions;
import io.openlineage.spark.agent.facets.SparkVersionFacet;
import io.openlineage.spark.api.OpenLineageContext;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.scheduler.JobSucceeded$;
import org.apache.spark.scheduler.SparkListenerJobEnd;
import org.apache.spark.scheduler.SparkListenerJobStart;
import org.apache.spark.scheduler.SparkListenerStageCompleted;
import org.apache.spark.scheduler.SparkListenerStageSubmitted;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import scala.collection.Seq$;

class SparkVersionFacetBuilderTest {

  private static SparkContext sparkContext;

  @BeforeAll
  public static void setupSparkContext() {
    sparkContext =
        SparkContext.getOrCreate(
            new SparkConf().setAppName("SparkVersionFacetBuilderTest").setMaster("local"));
  }

  @AfterAll
  public static void tearDownSparkContext() {
    sparkContext.stop();
  }

  @Test
  public void testIsDefinedForSparkListenerEvents() {
    SparkVersionFacetBuilder builder =
        new SparkVersionFacetBuilder(
            OpenLineageContext.builder()
                .sparkContext(sparkContext)
                .openLineage(new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI))
                .build());
    assertThat(builder.isDefinedAt(new SparkListenerSQLExecutionEnd(1, 1L))).isTrue();
    assertThat(
            builder.isDefinedAt(
                new SparkListenerSQLExecutionStart(1L, "abc", "abc", "abc", null, 1L)))
        .isTrue();
    assertThat(
            builder.isDefinedAt(
                new SparkListenerJobStart(1, 1L, Seq$.MODULE$.empty(), new Properties())))
        .isTrue();
    assertThat(builder.isDefinedAt(new SparkListenerJobEnd(1, 1L, JobSucceeded$.MODULE$))).isTrue();
    assertThat(builder.isDefinedAt(new SparkListenerStageSubmitted(null, new Properties())))
        .isTrue();
    assertThat(builder.isDefinedAt(new SparkListenerStageCompleted(null))).isTrue();
  }

  @Test
  public void testBuild() {
    SparkVersionFacetBuilder builder =
        new SparkVersionFacetBuilder(
            OpenLineageContext.builder()
                .sparkContext(sparkContext)
                .openLineage(new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI))
                .build());

    Map<String, RunFacet> runFacetMap = new HashMap<>();
    builder.build(new SparkListenerSQLExecutionEnd(1, 1L), runFacetMap::put);
    assertThat(runFacetMap)
        .hasEntrySatisfying(
            "spark_version",
            facet ->
                assertThat(facet)
                    .isInstanceOf(SparkVersionFacet.class)
                    .hasFieldOrPropertyWithValue("sparkVersion", sparkContext.version()));
  }
}
