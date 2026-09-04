/*
/* Copyright 2018-2026 contributors to the OpenLineage project
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
import io.openlineage.spark.agent.util.DatasetDispatchTrace;
import io.openlineage.spark.agent.util.PlanUtils;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.OpenLineageEventHandlerFactory;
import io.openlineage.spark.api.SparkOpenLineageConfig;
import io.openlineage.spark.api.TimeoutConfig;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import lombok.SneakyThrows;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;
import org.apache.spark.SparkContext;
import org.apache.spark.scheduler.ActiveJob;
import org.apache.spark.scheduler.Stage;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import scala.PartialFunction;
import scala.collection.JavaConverters;

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
          Thread.sleep(600L);
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
                openLineage.newParentRunFacetRun(UUID.randomUUID(), null),
                openLineage.newParentRunFacetJob("ns", "jobName", null),
                openLineage.newParentRunFacetRoot(
                    openLineage.newRootRun(UUID.randomUUID(), null),
                    openLineage.newRootJob("ns", "rootJobName", null))));
    when(runEventContext.loadNodes(anyMap(), anyMap()))
        .thenReturn(Collections.singletonList(mock(SparkListenerSQLExecutionEnd.class)));
    when(config.getCircuitBreaker()).thenReturn(circuitBreakerConfig);
    when(circuitBreakerConfig.getTimeout()).thenReturn(Optional.of(Duration.ofMillis(1000)));
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

  @Test
  void tracesBothDatasetPhasesInsideTimeoutWorkers() {
    Logger logger = Logger.getLogger(DatasetDispatchTrace.class);
    Level previousLevel = logger.getLevel();
    List<String> messages = new CopyOnWriteArrayList<>();
    List<String> threads = new CopyOnWriteArrayList<>();
    AppenderSkeleton appender =
        new AppenderSkeleton() {
          @Override
          protected void append(LoggingEvent event) {
            messages.add(event.getRenderedMessage());
            threads.add(event.getThreadName());
          }

          @Override
          public void close() {}

          @Override
          public boolean requiresLayout() {
            return false;
          }
        };
    logger.addAppender(appender);
    logger.setLevel(Level.TRACE);
    try {
      when(config.getTimeoutConfig()).thenReturn(new TimeoutConfig(100, 100));
      when(circuitBreakerConfig.getTimeout()).thenReturn(Optional.of(Duration.ofSeconds(5)));
      when(runEventContext.getEvent()).thenReturn(mock(SparkListenerSQLExecutionEnd.class));
      PartialFunction<Object, List<InputDataset>> input =
          new scala.runtime.AbstractPartialFunction<Object, List<InputDataset>>() {
            @Override
            public boolean isDefinedAt(Object event) {
              return true;
            }

            @Override
            public List<InputDataset> apply(Object event) {
              return Collections.emptyList();
            }
          };
      PartialFunction<Object, List<InputDataset>> delegating =
          new scala.runtime.AbstractPartialFunction<Object, List<InputDataset>>() {
            @Override
            public boolean isDefinedAt(Object event) {
              return true;
            }

            @Override
            public List<InputDataset> apply(Object event) {
              return new java.util.ArrayList<>(
                  PlanUtils.merge(Collections.singletonList(input)).apply(event));
            }
          };
      PartialFunction<Object, List<OpenLineage.OutputDataset>> output =
          new scala.runtime.AbstractPartialFunction<Object, List<OpenLineage.OutputDataset>>() {
            @Override
            public boolean isDefinedAt(Object event) {
              return true;
            }

            @Override
            public List<OpenLineage.OutputDataset> apply(Object event) {
              return Collections.emptyList();
            }
          };
      when(openLineageEventHandlerFactory.createInputDatasetBuilder(openLineageContext))
          .thenReturn(Collections.singletonList(delegating));
      when(openLineageEventHandlerFactory.createOutputDatasetBuilder(openLineageContext))
          .thenReturn(Collections.singletonList(output));

      RunEvent result =
          new OpenLineageRunEventBuilder(openLineageContext, openLineageEventHandlerFactory)
              .buildRun(runEventContext);

      assertThat(result.getInputs()).isEmpty();
      assertThat(result.getOutputs()).isEmpty();
      assertThat(messages)
          .anyMatch(message -> message.contains("phase=input") && message.contains("parent=2"));
      assertThat(messages)
          .anyMatch(
              message -> message.contains("phase=output") && message.contains("operation=apply"));
      assertThat(messages)
          .allMatch(message -> message.contains("run=" + openLineageContext.getRunUuid()));
      assertThat(threads).doesNotContain(Thread.currentThread().getName());
    } finally {
      logger.removeAppender(appender);
      logger.setLevel(previousLevel);
      appender.close();
    }
  }

  @Test
  void testEvictJobRemovesActiveJobAndStages() {
    OpenLineageRunEventBuilder builder =
        new OpenLineageRunEventBuilder(openLineageContext, openLineageEventHandlerFactory);
    ActiveJob activeJob = mock(ActiveJob.class);
    Stage finalStage = mock(Stage.class);
    Stage parentStage = mock(Stage.class);
    when(activeJob.jobId()).thenReturn(7);
    when(activeJob.finalStage()).thenReturn(finalStage);
    when(finalStage.id()).thenReturn(70);
    when(parentStage.id()).thenReturn(71);
    when(finalStage.parents())
        .thenReturn(
            JavaConverters.asScalaBufferConverter(Collections.singletonList(parentStage))
                .asScala()
                .toList());

    builder.registerJob(activeJob);

    assertThat(builder.getRetainedJobCount()).isOne();
    assertThat(builder.getRetainedStageCount()).isEqualTo(2);

    builder.evictJob(7);

    assertThat(builder.getRetainedJobCount()).isZero();
    assertThat(builder.getRetainedStageCount()).isZero();
  }
}
