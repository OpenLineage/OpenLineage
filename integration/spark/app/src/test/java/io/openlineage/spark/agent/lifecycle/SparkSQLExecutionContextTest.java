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

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.RunEvent;
import io.openlineage.client.OpenLineage.RunEvent.EventType;
import io.openlineage.spark.agent.EventEmitter;
import io.openlineage.spark.agent.SparkAgentTestExtension;
import io.openlineage.spark.agent.Versions;
import io.openlineage.spark.agent.filters.EventFilterUtils;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.OpenLineageEventHandlerFactory;
import java.util.Optional;
import org.apache.spark.scheduler.JobFailed;
import org.apache.spark.scheduler.SparkListenerJobEnd;
import org.apache.spark.scheduler.SparkListenerJobStart;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.QueryExecution;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;

@ExtendWith(SparkAgentTestExtension.class)
public class SparkSQLExecutionContextTest {

  private final long executionId = 1L;
  private final OpenLineageContext olContext = mock(OpenLineageContext.class);
  private final OpenLineage openLineage = new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI);
  private final EventEmitter eventEmitter = mock(EventEmitter.class);
  private final SparkSQLExecutionContext context =
      new SparkSQLExecutionContext(
          executionId,
          eventEmitter,
          olContext,
          new OpenLineageRunEventBuilder(olContext, mock(OpenLineageEventHandlerFactory.class)));
  private final QueryExecution queryExecution = mock(QueryExecution.class, RETURNS_DEEP_STUBS);

  @BeforeEach
  public void setup() {
    when(olContext.getQueryExecution()).thenReturn(Optional.of(queryExecution));
    when(olContext.getOpenLineage()).thenReturn(openLineage);
    when(eventEmitter.getAppName()).thenReturn(Optional.of("app-name"));
    when(queryExecution.executedPlan().nodeName()).thenReturn("some-node-name");
  }

  @Test
  void testSingeStartIsSent(SparkSession spark) {
    ArgumentCaptor<RunEvent> lineageEvent = ArgumentCaptor.forClass(OpenLineage.RunEvent.class);
    try (MockedStatic mocked = mockStatic(EventFilterUtils.class)) {
      when(EventFilterUtils.isDisabled(any(), any())).thenReturn(false);
      when(queryExecution.sparkPlan().sparkContext()).thenReturn(spark.sparkContext());

      context.start(mock(SparkListenerSQLExecutionStart.class));
      context.start(mock(SparkListenerJobStart.class));
    }
    verify(eventEmitter, times(2)).emit(lineageEvent.capture());

    assertThat(lineageEvent.getAllValues().get(0))
        .hasFieldOrPropertyWithValue("eventType", EventType.START);
    assertThat(lineageEvent.getAllValues().get(1))
        .hasFieldOrPropertyWithValue("eventType", EventType.RUNNING);
  }

  @Test
  void testSingeStartIsSentWhenJobStartGoesFirst(SparkSession spark) {
    ArgumentCaptor<RunEvent> lineageEvent = ArgumentCaptor.forClass(OpenLineage.RunEvent.class);
    try (MockedStatic mocked = mockStatic(EventFilterUtils.class)) {
      when(EventFilterUtils.isDisabled(any(), any())).thenReturn(false);
      when(queryExecution.sparkPlan().sparkContext()).thenReturn(spark.sparkContext());

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
    try (MockedStatic mocked = mockStatic(EventFilterUtils.class)) {
      when(EventFilterUtils.isDisabled(any(), any())).thenReturn(false);
      when(queryExecution.sparkPlan().sparkContext()).thenReturn(spark.sparkContext());

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
    try (MockedStatic mocked = mockStatic(EventFilterUtils.class)) {
      when(EventFilterUtils.isDisabled(any(), any())).thenReturn(false);
      when(queryExecution.sparkPlan().sparkContext()).thenReturn(spark.sparkContext());

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
    try (MockedStatic mocked = mockStatic(EventFilterUtils.class)) {
      when(EventFilterUtils.isDisabled(any(), any())).thenReturn(false);
      when(queryExecution.sparkPlan().sparkContext()).thenReturn(spark.sparkContext());

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
    try (MockedStatic mocked = mockStatic(EventFilterUtils.class)) {
      when(EventFilterUtils.isDisabled(any(), any())).thenReturn(false);
      when(queryExecution.sparkPlan().sparkContext()).thenReturn(spark.sparkContext());

      context.start(mock(SparkListenerJobStart.class));
      context.end(jobEnd);
    }
    verify(eventEmitter, times(2)).emit(lineageEvent.capture());

    assertThat(lineageEvent.getAllValues().get(0))
        .hasFieldOrPropertyWithValue("eventType", EventType.START);
    assertThat(lineageEvent.getAllValues().get(1))
        .hasFieldOrPropertyWithValue("eventType", EventType.FAIL);
  }
}
