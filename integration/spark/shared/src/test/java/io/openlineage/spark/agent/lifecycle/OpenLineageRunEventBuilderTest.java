/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.InputDataset;
import io.openlineage.client.OpenLineage.JobFacetsBuilder;
import io.openlineage.client.OpenLineage.RunEvent;
import io.openlineage.client.OpenLineage.RunFacetsBuilder;
import io.openlineage.client.circuitBreaker.TimeoutCircuitBreakerConfig;
import io.openlineage.spark.agent.Versions;
import io.openlineage.spark.agent.facets.DebugRunFacet;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.OpenLineageEventHandlerFactory;
import io.openlineage.spark.api.SparkOpenLineageConfig;
import io.openlineage.spark.api.TimeoutConfig;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import lombok.SneakyThrows;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import scala.PartialFunction;

class OpenLineageRunEventBuilderTest {

  public static final String DEBUG = "debug";
  SparkSession session = mock(SparkSession.class);
  OpenLineageContext openLineageContext;
  SparkOpenLineageConfig config = mock(SparkOpenLineageConfig.class, RETURNS_DEEP_STUBS);
  TimeoutCircuitBreakerConfig circuitBreakerConfig = mock(TimeoutCircuitBreakerConfig.class);
  OpenLineageEventHandlerFactory openLineageEventHandlerFactory =
      mock(OpenLineageEventHandlerFactory.class);
  OpenLineageRunEventContext runEventContext = mock(OpenLineageRunEventContext.class);
  OpenLineage openLineage;

  PartialFunction<Object, List<InputDataset>> timeoutInputDatasetBuilder =
      new PartialFunction<Object, List<InputDataset>>() {
        @Override
        @SneakyThrows
        public List<InputDataset> apply(Object v1) {
          Thread.sleep(60L);
          return Collections.emptyList();
        }

        @Override
        public boolean isDefinedAt(Object x) {
          return true;
        }
      };

  @BeforeEach
  public void beforeEach() {
    openLineage = new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI);
    SparkContext sparkContext = mock(SparkContext.class);
    when(session.sparkContext()).thenReturn(sparkContext);
    openLineageContext =
        OpenLineageContext.builder()
            .sparkSession(session)
            .sparkContext(sparkContext)
            .openLineage(openLineage)
            .applicationName("app-name")
            .applicationUuid(UUID.fromString("8d99e33e-bbbb-cccc-dddd-18f2343aaaaa"))
            .meterRegistry(new SimpleMeterRegistry())
            .openLineageConfig(config)
            .build();
    when(runEventContext.getJobFacetsBuilder())
        .thenReturn(
            new JobFacetsBuilder().sql(openLineage.newSQLJobFacet("SELECT * FROM table", "spark")));
    when(runEventContext.getRunFacetsBuilder()).thenReturn(new RunFacetsBuilder());
    when(runEventContext.getRunEventBuilder()).thenReturn(openLineage.newRunEventBuilder());
    when(runEventContext.getJobBuilder()).thenReturn(openLineage.newJobBuilder());
    when(runEventContext.getApplicationParentRunFacet())
        .thenReturn(
            openLineage.newParentRunFacet(
                openLineage.newParentRunFacetRun(UUID.randomUUID()),
                openLineage.newParentRunFacetJob("ns", "jobName"),
                openLineage.newParentRunFacetRoot(
                    openLineage.newRootRun(UUID.randomUUID()),
                    openLineage.newRootJob("ns", "rootJobName"))));
    when(runEventContext.loadNodes(anyMap(), anyMap()))
        .thenReturn(Collections.singletonList(mock(SparkListenerSQLExecutionEnd.class)));
    when(config.getCircuitBreaker()).thenReturn(circuitBreakerConfig);
    when(circuitBreakerConfig.getTimeout()).thenReturn(Optional.of(Duration.ofMillis(100)));
    when(openLineageEventHandlerFactory.createInputDatasetBuilder(openLineageContext))
        .thenReturn(Collections.singletonList(timeoutInputDatasetBuilder));
  }

  @Test
  void testBuildRunEventWithDatasetTimeout() {
    when(config.getTimeoutConfig()).thenReturn(new TimeoutConfig(50, 100));
    RunEvent event =
        new OpenLineageRunEventBuilder(openLineageContext, openLineageEventHandlerFactory)
            .buildRun(runEventContext);

    assertThat(event.getRun().getFacets().getAdditionalProperties()).containsKey(DEBUG);

    DebugRunFacet facet =
        (DebugRunFacet) event.getRun().getFacets().getAdditionalProperties().get(DEBUG);

    // assert job facets are present
    assertThat(event.getJob().getFacets().getSql()).isNotNull();

    // other facets like run facets should be present
    assertThat(event.getRun().getFacets().getParent()).isNotNull();

    // test that the debug facet contains the timeout message
    assertThat(facet.getLogs().get(0)).startsWith("Incomplete lineage:");
  }

  @Test
  void testBuildRunEventWithFacetsTimeout() {
    when(config.getTimeoutConfig()).thenReturn(new TimeoutConfig(null, 50));
    RunEvent event =
        new OpenLineageRunEventBuilder(openLineageContext, openLineageEventHandlerFactory)
            .buildRun(runEventContext);

    assertThat(event.getRun().getFacets().getAdditionalProperties()).containsKey(DEBUG);

    DebugRunFacet facet =
        (DebugRunFacet) event.getRun().getFacets().getAdditionalProperties().get(DEBUG);

    // assert job facets is not null -> built before timeout
    assertThat(event.getJob().getFacets().getSql()).isNotNull();

    // parent shall be null
    assertThat(event.getRun().getFacets().getParent()).isNull();

    // test that the debug facet contains the timeout message
    assertThat(facet.getLogs().get(0)).startsWith("Incomplete lineage:");
  }
}
