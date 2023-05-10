/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink;

import static io.openlineage.flink.OpenLineageFlinkJobListener.DEFAULT_JOB_NAMESPACE;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.openlineage.flink.tracker.OpenLineageContinousJobTracker;
import io.openlineage.flink.visitor.lifecycle.FlinkExecutionContext;
import io.openlineage.flink.visitor.lifecycle.FlinkExecutionContextFactory;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import lombok.SneakyThrows;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraphGenerator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

class JobListenerTest {

  JobClient jobClient = mock(JobClient.class);
  JobID jobId = mock(JobID.class);
  List<Transformation<?>> transformations = new ArrayList<>();
  OpenLineageFlinkJobListener listener;
  FlinkExecutionContext context = mock(FlinkExecutionContext.class);
  ReadableConfig readableConfig = mock(ReadableConfig.class);

  String jobName = "some-job-name";

  String jobNamespace = "some-job-namespace";

  @BeforeEach
  @SneakyThrows
  public void setup() {
    transformations.add(mock(Transformation.class));
    when(jobClient.getJobID()).thenReturn(jobId);
  }

  @Test
  @SneakyThrows
  void testOnJobSubmitted() {
    StreamExecutionEnvironment streamExecutionEnvironment = new StreamExecutionEnvironment();
    FieldUtils.writeField(
        FieldUtils.getField(StreamExecutionEnvironment.class, "transformations", true),
        streamExecutionEnvironment,
        transformations,
        true);

    OpenLineageContinousJobTracker tracker = mock(OpenLineageContinousJobTracker.class);
    doNothing().when(tracker).startTracking(context);

    listener =
        OpenLineageFlinkJobListener.builder()
            .executionEnvironment(streamExecutionEnvironment)
            .jobTracker(tracker)
            .jobNamespace(jobNamespace)
            .jobName(jobName)
            .build();

    try (MockedStatic<FlinkExecutionContextFactory> contextFactory =
        mockStatic(FlinkExecutionContextFactory.class)) {
      when(FlinkExecutionContextFactory.getContext(
              eq(jobNamespace), eq(jobName), eq(jobId), eq(transformations)))
          .thenReturn(context);
      doNothing().when(context).onJobSubmitted();

      listener.onJobSubmitted(jobClient, null);
      verify(context, times(1)).onJobSubmitted();
      verify(tracker, times(1)).startTracking(context);
    }
  }

  @SuppressWarnings("PMD")
  @Test
  @SneakyThrows
  void testOnJobExecuted() {
    StreamExecutionEnvironment streamExecutionEnvironment = mock(StreamExecutionEnvironment.class);
    ExecutionConfig.GlobalJobParameters globalJobParameters =
        mock(ExecutionConfig.GlobalJobParameters.class);
    ExecutionConfig executionConfig = mock(ExecutionConfig.class);
    when(streamExecutionEnvironment.getConfiguration()).thenReturn(readableConfig);
    when(streamExecutionEnvironment.getConfig()).thenReturn(executionConfig);
    when(executionConfig.getGlobalJobParameters()).thenReturn(globalJobParameters);
    when(globalJobParameters.toMap()).thenReturn(new HashMap<>());

    OpenLineageContinousJobTracker tracker = mock(OpenLineageContinousJobTracker.class);
    JobExecutionResult jobExecutionResult = mock(JobExecutionResult.class);
    when(jobExecutionResult.getJobID()).thenReturn(jobId);
    doNothing().when(context).onJobSubmitted();
    doNothing().when(tracker).startTracking(context);

    try (MockedStatic<FlinkExecutionContextFactory> contextFactory =
        mockStatic(FlinkExecutionContextFactory.class)) {
      when(FlinkExecutionContextFactory.getContext(
              eq(jobNamespace), eq(jobName), eq(jobId), eq(transformations)))
          .thenReturn(context);
      doNothing().when(context).onJobSubmitted();

      listener =
          OpenLineageFlinkJobListener.builder()
              .executionEnvironment(streamExecutionEnvironment)
              .jobTracker(tracker)
              .build();
      listener.onJobSubmitted(jobClient, null);
      listener.onJobExecuted(jobExecutionResult, null);
    }
  }

  @Test
  @SneakyThrows
  void testOnJobSubmittedWithDefaultNamespaceAndName() {
    StreamExecutionEnvironment streamExecutionEnvironment = new StreamExecutionEnvironment();
    FieldUtils.writeField(
        FieldUtils.getField(StreamExecutionEnvironment.class, "transformations", true),
        streamExecutionEnvironment,
        transformations,
        true);

    OpenLineageContinousJobTracker tracker = mock(OpenLineageContinousJobTracker.class);
    doNothing().when(tracker).startTracking(context);

    listener =
        OpenLineageFlinkJobListener.builder()
            .executionEnvironment(streamExecutionEnvironment)
            .jobTracker(tracker)
            .build();

    try (MockedStatic<FlinkExecutionContextFactory> contextFactory =
        mockStatic(FlinkExecutionContextFactory.class)) {
      when(FlinkExecutionContextFactory.getContext(
              eq(DEFAULT_JOB_NAMESPACE),
              eq(StreamGraphGenerator.DEFAULT_STREAMING_JOB_NAME),
              eq(jobId),
              eq(transformations)))
          .thenReturn(context);
      doNothing().when(context).onJobSubmitted();

      listener.onJobSubmitted(jobClient, null);
      verify(context, times(1)).onJobSubmitted();
      verify(tracker, times(1)).startTracking(context);
    }
  }
}
