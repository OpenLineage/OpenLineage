/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.DefaultJobFacet;
import io.openlineage.client.OpenLineage.DefaultRunFacet;
import io.openlineage.client.OpenLineage.JobFacet;
import io.openlineage.client.OpenLineage.RunEvent;
import io.openlineage.client.OpenLineage.RunEvent.EventType;
import io.openlineage.client.OpenLineage.RunFacet;
import io.openlineage.spark.agent.EventEmitter;
import io.openlineage.spark.agent.SparkAgentTestExtension;
import io.openlineage.spark.agent.Versions;
import io.openlineage.spark.agent.filters.EventFilterUtils;
import io.openlineage.spark.api.CustomFacetBuilder;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.OpenLineageEventHandlerFactory;
import io.openlineage.spark.api.OpenLineageRunStatus;
import io.openlineage.spark.api.SparkOpenLineageConfig;
import io.openlineage.spark.api.VisitedNodes;
import java.util.Collections;
import java.util.Optional;
import java.util.UUID;
import java.util.function.BiConsumer;
import org.apache.spark.scheduler.SparkListenerApplicationEnd;
import org.apache.spark.scheduler.SparkListenerApplicationStart;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

@ExtendWith(SparkAgentTestExtension.class)
class SparkApplicationExecutionContextTest {

  private final OpenLineageContext olContext = mock(OpenLineageContext.class);
  private final OpenLineage openLineage = new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI);
  private final EventEmitter eventEmitter = mock(EventEmitter.class);
  private final OpenLineageEventHandlerFactory eventHandlerFactory =
      mock(OpenLineageEventHandlerFactory.class);
  private SparkApplicationExecutionContext context =
      new SparkApplicationExecutionContext(
          eventEmitter, olContext, new OpenLineageRunEventBuilder(olContext, eventHandlerFactory));

  @AfterEach
  void reset() {
    Mockito.reset(olContext, eventEmitter);
  }

  @BeforeEach
  void setup(SparkSession spark) {
    when(olContext.getOpenLineage()).thenReturn(openLineage);
    when(olContext.getSparkContext()).thenReturn(Optional.of(spark.sparkContext()));
    when(olContext.getOpenLineageConfig()).thenReturn(new SparkOpenLineageConfig());
    when(olContext.getLineageRunStatus()).thenReturn(new OpenLineageRunStatus());
    when(olContext.getMeterRegistry()).thenReturn(new SimpleMeterRegistry());
    when(olContext.getVisitedNodes()).thenReturn(new VisitedNodes());
    when(olContext.getApplicationUuid())
        .thenReturn(UUID.fromString("993426b3-1ca7-44af-8473-8e58c757ebd1"));

    when(eventEmitter.getOverriddenAppName()).thenReturn(Optional.of("app-name"));
    when(eventEmitter.getApplicationRunId())
        .thenReturn(UUID.fromString("993426b3-1ca7-44af-8473-8e58c757ebd1"));

    when(eventEmitter.getJobNamespace()).thenReturn("ns_name");
    when(eventEmitter.getParentJobName()).thenReturn(Optional.of("parent_name"));
    when(eventEmitter.getParentJobNamespace()).thenReturn(Optional.of("parent_namespace"));
    when(eventEmitter.getParentRunId())
        .thenReturn(Optional.of(UUID.fromString("4e948e60-1639-4796-950e-cdc6d45915f4")));
  }

  @Test
  void testSingleStartIsSent(SparkSession spark) {
    ArgumentCaptor<RunEvent> lineageEvent = ArgumentCaptor.forClass(OpenLineage.RunEvent.class);
    try (MockedStatic<EventFilterUtils> ignored = mockStatic(EventFilterUtils.class)) {
      when(EventFilterUtils.isDisabled(any(), any())).thenReturn(false);
    }

    context.start(mock(SparkListenerApplicationStart.class));
    verify(eventEmitter, times(1)).emit(lineageEvent.capture());

    RunEvent runEvent = lineageEvent.getAllValues().get(0);
    assertThat(runEvent).hasFieldOrPropertyWithValue("eventType", EventType.START);
  }

  @Test
  void testSingleEndIsSent(SparkSession spark) {
    ArgumentCaptor<RunEvent> lineageEvent = ArgumentCaptor.forClass(OpenLineage.RunEvent.class);
    try (MockedStatic<EventFilterUtils> ignored = mockStatic(EventFilterUtils.class)) {
      when(EventFilterUtils.isDisabled(any(), any())).thenReturn(false);

      context.end(mock(SparkListenerApplicationEnd.class));
    }
    verify(eventEmitter, times(1)).emit(lineageEvent.capture());

    assertThat(lineageEvent.getAllValues().get(0))
        .hasFieldOrPropertyWithValue("eventType", EventType.COMPLETE);
  }

  @Test
  void testRunAndJobAreSet(SparkSession spark) {
    ArgumentCaptor<RunEvent> lineageEvent = ArgumentCaptor.forClass(OpenLineage.RunEvent.class);
    try (MockedStatic<EventFilterUtils> ignored = mockStatic(EventFilterUtils.class)) {
      when(EventFilterUtils.isDisabled(any(), any())).thenReturn(false);

      context.start(mock(SparkListenerApplicationStart.class));
      context.end(mock(SparkListenerApplicationEnd.class));
    }
    verify(eventEmitter, times(2)).emit(lineageEvent.capture());

    for (RunEvent runEvent : lineageEvent.getAllValues()) {
      OpenLineage.Run run = runEvent.getRun();
      OpenLineage.Job job = runEvent.getJob();
      assertThat(job.getName()).isEqualTo("setup"); // spark agent extension caller
      assertThat(job.getNamespace()).isEqualTo("ns_name");
      assertThat(run.getRunId()).isEqualTo(UUID.fromString("993426b3-1ca7-44af-8473-8e58c757ebd1"));
    }
    assertThat(lineageEvent.getAllValues()).isNotEmpty();
  }

  @Test
  void testParentRunFacetIsSet(SparkSession spark) {
    ArgumentCaptor<RunEvent> lineageEvent = ArgumentCaptor.forClass(OpenLineage.RunEvent.class);
    try (MockedStatic<EventFilterUtils> ignored = mockStatic(EventFilterUtils.class)) {
      when(EventFilterUtils.isDisabled(any(), any())).thenReturn(false);

      context.start(mock(SparkListenerApplicationStart.class));
      context.end(mock(SparkListenerApplicationEnd.class));
    }
    verify(eventEmitter, times(2)).emit(lineageEvent.capture());

    for (RunEvent runEvent : lineageEvent.getAllValues()) {
      OpenLineage.ParentRunFacet parentRunFacet = runEvent.getRun().getFacets().getParent();
      OpenLineage.ParentRunFacetJob parentJob = parentRunFacet.getJob();
      OpenLineage.ParentRunFacetRun parentRun = parentRunFacet.getRun();
      assertThat(parentJob.getName()).isEqualTo("parent_name");
      assertThat(parentJob.getNamespace()).isEqualTo("parent_namespace");
      assertThat(parentRun.getRunId())
          .isEqualTo(UUID.fromString("4e948e60-1639-4796-950e-cdc6d45915f4"));
    }
    assertThat(lineageEvent.getAllValues()).isNotEmpty();
  }

  @Test
  void testJobTypeJobFacetIsSet(SparkSession spark) {
    ArgumentCaptor<RunEvent> lineageEvent = ArgumentCaptor.forClass(OpenLineage.RunEvent.class);
    try (MockedStatic<EventFilterUtils> ignored = mockStatic(EventFilterUtils.class)) {
      when(EventFilterUtils.isDisabled(any(), any())).thenReturn(false);

      context.start(mock(SparkListenerApplicationStart.class));
      context.end(mock(SparkListenerApplicationEnd.class));
    }
    verify(eventEmitter, times(2)).emit(lineageEvent.capture());

    for (RunEvent runEvent : lineageEvent.getAllValues()) {
      OpenLineage.Job job = runEvent.getJob();
      OpenLineage.JobTypeJobFacet jobTypeFacet = job.getFacets().getJobType();
      assertThat(jobTypeFacet.getJobType()).isEqualTo("APPLICATION");
      assertThat(jobTypeFacet.getProcessingType()).isEqualTo("NONE");
      assertThat(jobTypeFacet.getIntegration()).isEqualTo("SPARK");
    }
    assertThat(lineageEvent.getAllValues()).isNotEmpty();
  }

  @Test
  void testCustomFacetBuildersAreCalled(SparkSession spark) {
    when(eventHandlerFactory.createJobFacetBuilders(olContext))
        .thenReturn(
            Collections.singletonList(
                new CustomFacetBuilder<Object, JobFacet>() {
                  @Override
                  protected void build(
                      Object event, BiConsumer<String, ? super JobFacet> consumer) {
                    consumer.accept(
                        "custom_job_facet",
                        new DefaultJobFacet(Versions.OPEN_LINEAGE_PRODUCER_URI, false));
                  }
                }));

    when(eventHandlerFactory.createRunFacetBuilders(olContext))
        .thenReturn(
            Collections.singletonList(
                new CustomFacetBuilder<Object, RunFacet>() {
                  @Override
                  protected void build(
                      Object event, BiConsumer<String, ? super RunFacet> consumer) {
                    consumer.accept(
                        "custom_run_facet",
                        new DefaultRunFacet(Versions.OPEN_LINEAGE_PRODUCER_URI));
                  }
                }));

    // need to new context to inject eventHandlerFactory
    SparkApplicationExecutionContext context =
        new SparkApplicationExecutionContext(
            eventEmitter,
            olContext,
            new OpenLineageRunEventBuilder(olContext, eventHandlerFactory));

    ArgumentCaptor<RunEvent> lineageEvent = ArgumentCaptor.forClass(OpenLineage.RunEvent.class);
    try (MockedStatic<EventFilterUtils> ignored = mockStatic(EventFilterUtils.class)) {
      when(EventFilterUtils.isDisabled(any(), any())).thenReturn(false);

      context.start(mock(SparkListenerApplicationStart.class));
      context.end(mock(SparkListenerApplicationEnd.class));
    }
    verify(eventEmitter, times(2)).emit(lineageEvent.capture());

    for (RunEvent runEvent : lineageEvent.getAllValues()) {
      assertThat(runEvent.getRun().getFacets().getAdditionalProperties())
          .containsKey("custom_run_facet");
      assertThat(runEvent.getJob().getFacets().getAdditionalProperties())
          .containsKey("custom_job_facet");
    }
    assertThat(lineageEvent.getAllValues()).isNotEmpty();
  }
}
