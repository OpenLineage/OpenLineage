/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.facets.builder;

import static org.assertj.core.api.Assertions.assertThat;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.OutputDatasetFacet;
import io.openlineage.spark.agent.JobMetricsHolder;
import io.openlineage.spark.agent.Versions;
import io.openlineage.spark.api.OpenLineageContext;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.executor.TaskMetrics;
import org.apache.spark.scheduler.JobSucceeded$;
import org.apache.spark.scheduler.SparkListenerJobEnd;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class OutputStatisticsOutputDatasetFacetBuilderTest {

  private static SparkContext sparkContext;

  @BeforeAll
  public static void setup() {
    sparkContext =
        SparkContext.getOrCreate(
            new SparkConf()
                .setAppName("OutputStatisticsOutputDatasetFacetBuilderTest")
                .setMaster("local"));
  }

  @AfterAll
  public static void tearDown() {
    sparkContext.stop();
  }

  @Test
  void testIsDefined() {
    OutputStatisticsOutputDatasetFacetBuilder builder =
        new OutputStatisticsOutputDatasetFacetBuilder(
            OpenLineageContext.builder()
                .openLineage(new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI))
                .sparkContext(sparkContext)
                .build());
    assertThat(builder.isDefinedAt(new SparkListenerJobEnd(1, 1L, JobSucceeded$.MODULE$))).isTrue();
    assertThat(builder.isDefinedAt(new SparkListenerSQLExecutionEnd(1L, 1L))).isFalse();
  }

  @Test
  void testBuild() {
    OutputStatisticsOutputDatasetFacetBuilder builder =
        new OutputStatisticsOutputDatasetFacetBuilder(
            OpenLineageContext.builder()
                .openLineage(new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI))
                .sparkContext(sparkContext)
                .build());
    JobMetricsHolder.getInstance().addJobStages(1, Collections.singleton(1));
    TaskMetrics taskMetrics = new TaskMetrics();
    taskMetrics.outputMetrics().setBytesWritten(10L);
    taskMetrics.outputMetrics().setRecordsWritten(100L);
    JobMetricsHolder.getInstance().addMetrics(1, taskMetrics);

    Map<String, OutputDatasetFacet> facetsMap = new HashMap<>();
    builder.build(new SparkListenerJobEnd(1, 1L, JobSucceeded$.MODULE$), facetsMap::put);
    assertThat(facetsMap)
        .hasEntrySatisfying(
            "outputStatistics",
            facet ->
                assertThat(facet)
                    .hasFieldOrPropertyWithValue("rowCount", 100L)
                    .hasFieldOrPropertyWithValue("size", 10L));
  }
}
