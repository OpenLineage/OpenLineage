/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.RunEvent;
import io.openlineage.client.OpenLineage.RunEvent.EventType;
import io.openlineage.spark.agent.EventEmitter;
import io.openlineage.spark.agent.SparkAgentTestExtension;
import io.openlineage.spark.agent.Versions;
import io.openlineage.spark.agent.filters.EventFilterUtils;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.OpenLineageEventHandlerFactory;
import io.openlineage.spark.api.SparkOpenLineageConfig;
import java.util.Optional;
import java.util.UUID;
import org.apache.spark.scheduler.JobFailed;
import org.apache.spark.scheduler.SparkListenerJobEnd;
import org.apache.spark.scheduler.SparkListenerJobStart;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.QueryExecution;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

@ExtendWith(SparkAgentTestExtension.class)
class SparkSQLExecutionContextTest {

  private final long executionId = 1L;
  private final OpenLineageContext olContext = mock(OpenLineageContext.class);
  private final OpenLineage openLineage = new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI);
  private final EventEmitter eventEmitter = mock(EventEmitter.class);
  private SparkSQLExecutionContext context;
  private final QueryExecution queryExecution = mock(QueryExecution.class, RETURNS_DEEP_STUBS);

  @AfterEach
  void reset() {
    Mockito.reset(olContext, eventEmitter, queryExecution);
  }

  @BeforeEach
  void setup(SparkSession spark) {
    when(olContext.getQueryExecution()).thenReturn(Optional.of(queryExecution));
    when(olContext.getSparkContext()).thenReturn(Optional.of(spark.sparkContext()));
    when(queryExecution.sparkPlan().sparkContext()).thenReturn(spark.sparkContext());
    when(olContext.getOpenLineage()).thenReturn(openLineage);
    when(olContext.getOpenLineageConfig()).thenReturn(new SparkOpenLineageConfig());
    when(olContext.getMeterRegistry()).thenReturn(new SimpleMeterRegistry());
    when(olContext.getRunUuid())
        .thenReturn(UUID.fromString("8d99e33e-2a1c-4254-9600-18f23435fc3b"));
    when(olContext.getOpenLineageConfig()).thenReturn(new SparkOpenLineageConfig());
    context =
        new SparkSQLExecutionContext(
            executionId,
            eventEmitter,
            olContext,
            new OpenLineageRunEventBuilder(olContext, mock(OpenLineageEventHandlerFactory.class)));

    when(eventEmitter.getOverriddenAppName()).thenReturn(Optional.of("test-rdd"));
    when(queryExecution.executedPlan().nodeName()).thenReturn("some-node-name");

    when(eventEmitter.getJobNamespace()).thenReturn("ns_name");
    when(eventEmitter.getApplicationRunId())
        .thenReturn(UUID.fromString("8d99e33e-bbbb-cccc-dddd-18f2343aaaaa"));
    when(eventEmitter.getApplicationJobName()).thenReturn("app-name");
  }

  @Test
  void testSingleStartIsSent(SparkSession spark) {
    ArgumentCaptor<RunEvent> lineageEvent = ArgumentCaptor.forClass(OpenLineage.RunEvent.class);
    try (MockedStatic<EventFilterUtils> ignored = mockStatic(EventFilterUtils.class)) {
      when(EventFilterUtils.isDisabled(any(), any())).thenReturn(false);
    }

    context.start(mock(SparkListenerSQLExecutionStart.class));
    context.start(mock(SparkListenerJobStart.class));
    verify(eventEmitter, times(2)).emit(lineageEvent.capture());

    assertThat(lineageEvent.getAllValues().get(0))
        .hasFieldOrPropertyWithValue("eventType", EventType.START);
    assertThat(lineageEvent.getAllValues().get(1))
        .hasFieldOrPropertyWithValue("eventType", EventType.RUNNING);
  }

  @Test
  void testSingleStartIsSentWhenJobStartGoesFirst(SparkSession spark) {
    ArgumentCaptor<RunEvent> lineageEvent = ArgumentCaptor.forClass(OpenLineage.RunEvent.class);
    try (MockedStatic<EventFilterUtils> ignored = mockStatic(EventFilterUtils.class)) {
      when(EventFilterUtils.isDisabled(any(), any())).thenReturn(false);

      context.start(mock(SparkListenerJobStart.class));
      context.start(mock(SparkListenerSQLExecutionStart.class));
    }
    verify(eventEmitter, times(2)).emit(lineageEvent.capture());

    assertThat(lineageEvent.getAllValues().get(0))
        .hasFieldOrPropertyWithValue("eventType", EventType.START);
    assertThat(lineageEvent.getAllValues().get(1))
        .hasFieldOrPropertyWithValue("eventType", EventType.RUNNING);
  }

  @Test
  void testSingleCompleteIsSent(SparkSession spark) {
    ArgumentCaptor<RunEvent> lineageEvent = ArgumentCaptor.forClass(OpenLineage.RunEvent.class);
    try (MockedStatic<EventFilterUtils> ignored = mockStatic(EventFilterUtils.class)) {
      when(EventFilterUtils.isDisabled(any(), any())).thenReturn(false);

      context.start(mock(SparkListenerJobStart.class));
      context.start(mock(SparkListenerSQLExecutionStart.class));
      context.end(mock(SparkListenerSQLExecutionEnd.class));
      context.end(mock(SparkListenerJobEnd.class));
    }
    verify(eventEmitter, times(4)).emit(lineageEvent.capture());

    assertThat(lineageEvent.getAllValues().get(2))
        .hasFieldOrPropertyWithValue("eventType", EventType.RUNNING);
    assertThat(lineageEvent.getAllValues().get(3))
        .hasFieldOrPropertyWithValue("eventType", EventType.COMPLETE);
  }

  @Test
  void testSingleCompleteIsSentWhenJobEndGoesFirst(SparkSession spark) {
    ArgumentCaptor<RunEvent> lineageEvent = ArgumentCaptor.forClass(OpenLineage.RunEvent.class);
    try (MockedStatic<EventFilterUtils> ignored = mockStatic(EventFilterUtils.class)) {
      when(EventFilterUtils.isDisabled(any(), any())).thenReturn(false);

      context.start(mock(SparkListenerJobStart.class));
      context.start(mock(SparkListenerSQLExecutionStart.class));
      context.end(mock(SparkListenerJobEnd.class));
      context.end(mock(SparkListenerSQLExecutionEnd.class));
    }
    verify(eventEmitter, times(4)).emit(lineageEvent.capture());

    assertThat(lineageEvent.getAllValues().get(2))
        .hasFieldOrPropertyWithValue("eventType", EventType.RUNNING);
    assertThat(lineageEvent.getAllValues().get(3))
        .hasFieldOrPropertyWithValue("eventType", EventType.COMPLETE);
  }

  @Test
  void testCompleteIsSentWhenNoSqlStart(SparkSession spark) {
    ArgumentCaptor<RunEvent> lineageEvent = ArgumentCaptor.forClass(OpenLineage.RunEvent.class);
    try (MockedStatic<EventFilterUtils> ignored = mockStatic(EventFilterUtils.class)) {
      when(EventFilterUtils.isDisabled(any(), any())).thenReturn(false);

      context.start(mock(SparkListenerJobStart.class));
      context.end(mock(SparkListenerJobEnd.class));
    }
    verify(eventEmitter, times(2)).emit(lineageEvent.capture());

    assertThat(lineageEvent.getAllValues().get(0))
        .hasFieldOrPropertyWithValue("eventType", EventType.START);
    assertThat(lineageEvent.getAllValues().get(1))
        .hasFieldOrPropertyWithValue("eventType", EventType.COMPLETE);
  }

  @Test
  void testFailIsSent(SparkSession spark) {
    ArgumentCaptor<RunEvent> lineageEvent = ArgumentCaptor.forClass(OpenLineage.RunEvent.class);
    SparkListenerJobEnd jobEnd = mock(SparkListenerJobEnd.class);
    when(jobEnd.jobResult()).thenReturn(mock(JobFailed.class));
    try (MockedStatic<EventFilterUtils> ignored = mockStatic(EventFilterUtils.class)) {
      when(EventFilterUtils.isDisabled(any(), any())).thenReturn(false);

      context.start(mock(SparkListenerJobStart.class));
      context.end(jobEnd);
    }
    verify(eventEmitter, times(2)).emit(lineageEvent.capture());

    assertThat(lineageEvent.getAllValues().get(0))
        .hasFieldOrPropertyWithValue("eventType", EventType.START);
    assertThat(lineageEvent.getAllValues().get(1))
        .hasFieldOrPropertyWithValue("eventType", EventType.FAIL);
  }

  @Test
  void testSingleCompleteIsSentWithJobType(SparkSession spark) {
    ArgumentCaptor<RunEvent> lineageEvent = ArgumentCaptor.forClass(OpenLineage.RunEvent.class);
    try (MockedStatic<EventFilterUtils> ignored = mockStatic(EventFilterUtils.class)) {
      when(EventFilterUtils.isDisabled(any(), any())).thenReturn(false);

      context.start(mock(SparkListenerJobStart.class));
      context.start(mock(SparkListenerSQLExecutionStart.class));
      context.end(mock(SparkListenerSQLExecutionEnd.class));
      context.end(mock(SparkListenerJobEnd.class));
    }
    verify(eventEmitter, times(4)).emit(lineageEvent.capture());

    for (RunEvent event : lineageEvent.getAllValues()) {
      OpenLineage.JobTypeJobFacet jobType = event.getJob().getFacets().getJobType();
      assertThat(jobType).isNotNull();
      assertThat(jobType.getJobType()).isEqualTo("SQL_JOB");
      assertThat(jobType.getIntegration()).isEqualTo("SPARK");
      assertThat(jobType.getProcessingType()).isEqualTo("BATCH");
    }
  }

  @Test
  void testSingleCompleteIsSentWithJobTypeStreaming(SparkSession spark) {
    ArgumentCaptor<RunEvent> lineageEvent = ArgumentCaptor.forClass(OpenLineage.RunEvent.class);
    try (MockedStatic<EventFilterUtils> ignored = mockStatic(EventFilterUtils.class)) {
      when(EventFilterUtils.isDisabled(any(), any())).thenReturn(false);
      when(queryExecution.optimizedPlan().isStreaming()).thenReturn(true);

      context.start(mock(SparkListenerJobStart.class));
      context.start(mock(SparkListenerSQLExecutionStart.class));
      context.end(mock(SparkListenerSQLExecutionEnd.class));
      context.end(mock(SparkListenerJobEnd.class));
    }
    verify(eventEmitter, times(4)).emit(lineageEvent.capture());

    for (RunEvent event : lineageEvent.getAllValues()) {
      OpenLineage.JobTypeJobFacet jobType = event.getJob().getFacets().getJobType();
      assertThat(jobType).isNotNull();
      assertThat(jobType.getJobType()).isEqualTo("SQL_JOB");
      assertThat(jobType.getIntegration()).isEqualTo("SPARK");
      assertThat(jobType.getProcessingType()).isEqualTo("STREAMING");
    }
  }

  @Test
  void testRunAndJobAreSet(SparkSession spark) {
    ArgumentCaptor<RunEvent> lineageEvent = ArgumentCaptor.forClass(OpenLineage.RunEvent.class);
    try (MockedStatic<EventFilterUtils> ignored = mockStatic(EventFilterUtils.class)) {
      when(EventFilterUtils.isDisabled(any(), any())).thenReturn(false);

      context.start(mock(SparkListenerJobStart.class));
      context.start(mock(SparkListenerSQLExecutionStart.class));
      context.end(mock(SparkListenerSQLExecutionEnd.class));
      context.end(mock(SparkListenerJobEnd.class));
    }

    for (RunEvent runEvent : lineageEvent.getAllValues()) {
      OpenLineage.Run run = runEvent.getRun();
      OpenLineage.Job job = runEvent.getJob();
      assertThat(job.getName()).isEqualTo("test_rdd.some_node_name");
      assertThat(job.getNamespace()).isEqualTo("ns_name");
      assertThat(run.getRunId()).isEqualTo(UUID.fromString("8d99e33e-2a1c-4254-9600-18f23435fc3b"));
    }
  }

  @Test
  void testParentRunFacetIsSet(SparkSession spark) {
    ArgumentCaptor<RunEvent> lineageEvent = ArgumentCaptor.forClass(OpenLineage.RunEvent.class);
    try (MockedStatic<EventFilterUtils> ignored = mockStatic(EventFilterUtils.class)) {
      when(EventFilterUtils.isDisabled(any(), any())).thenReturn(false);
      when(olContext.getSparkContext()).thenReturn(Optional.of(spark.sparkContext()));

      context.start(mock(SparkListenerJobStart.class));
      context.start(mock(SparkListenerSQLExecutionStart.class));
      context.end(mock(SparkListenerSQLExecutionEnd.class));
      context.end(mock(SparkListenerJobEnd.class));
    }

    for (RunEvent runEvent : lineageEvent.getAllValues()) {
      OpenLineage.ParentRunFacet parentRunFacet = runEvent.getRun().getFacets().getParent();
      OpenLineage.ParentRunFacetJob parentJob = parentRunFacet.getJob();
      OpenLineage.ParentRunFacetRun parentRun = parentRunFacet.getRun();
      assertThat(parentJob.getName()).isEqualTo("app_name");
      assertThat(parentJob.getNamespace()).isEqualTo("ns_name");
      assertThat(parentRun.getRunId())
          .isEqualTo(UUID.fromString("8d99e33e-bbbb-cccc-dddd-18f2343aaaaa"));
    }
  }
}
