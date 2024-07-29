/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.facets.builder;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.OutputDatasetFacet;
import io.openlineage.spark.agent.JobMetricsHolder;
import io.openlineage.spark.agent.Versions;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.SparkOpenLineageConfig;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.spark.SparkContext;
import org.apache.spark.executor.TaskMetrics;
import org.apache.spark.scheduler.JobSucceeded$;
import org.apache.spark.scheduler.SparkListenerJobEnd;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart;
import org.junit.jupiter.api.Test;

@SuppressWarnings("PMD")
class OutputStatisticsOutputDatasetFacetBuilderTest {

  private static SparkContext sparkContext = mock(SparkContext.class);

  @Test
  void testIsDefined() {
    OutputStatisticsOutputDatasetFacetBuilder builder =
        new OutputStatisticsOutputDatasetFacetBuilder(
            OpenLineageContext.builder()
                .openLineage(new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI))
                .sparkContext(sparkContext)
                .meterRegistry(new SimpleMeterRegistry())
                .openLineageConfig(new SparkOpenLineageConfig())
                .build());
    assertThat(builder.isDefinedAt(new SparkListenerJobEnd(1, 1L, JobSucceeded$.MODULE$))).isTrue();
    assertThat(builder.isDefinedAt(new SparkListenerSQLExecutionEnd(1L, 1L))).isTrue();
    assertThat(builder.isDefinedAt(mock(SparkListenerSQLExecutionStart.class))).isFalse();
  }

  @Test
  void testBuild() {
    OpenLineageContext context =
        OpenLineageContext.builder()
            .openLineage(new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI))
            .sparkContext(sparkContext)
            .meterRegistry(new SimpleMeterRegistry())
            .openLineageConfig(new SparkOpenLineageConfig())
            .activeJobId(1)
            .build();
    OutputStatisticsOutputDatasetFacetBuilder builder =
        new OutputStatisticsOutputDatasetFacetBuilder(context);
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
