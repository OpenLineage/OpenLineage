/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink;

import static io.openlineage.flink.OpenLineageFlinkJobListener.DEFAULT_JOB_NAMESPACE;
import static io.openlineage.flink.OpenLineageFlinkJobListener.OPENLINEAGE_LISTENER_CONFIG_DURATION;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.openlineage.flink.tracker.OpenLineageContinousJobTracker;
import io.openlineage.flink.tracker.OpenLineageContinousJobTrackerFactory;
import io.openlineage.flink.utils.JobTypeUtils;
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
import org.apache.flink.configuration.Configuration;
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
  OpenLineageContinousJobTracker tracker = mock(OpenLineageContinousJobTracker.class);
  Configuration readableConfig = new Configuration();
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
    StreamExecutionEnvironment streamExecutionEnvironment =
        new StreamExecutionEnvironment(readableConfig);
    FieldUtils.writeField(
        FieldUtils.getField(StreamExecutionEnvironment.class, "transformations", true),
        streamExecutionEnvironment,
        transformations,
        true);

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
              eq(readableConfig),
              eq(jobNamespace),
              eq(jobName),
              eq(jobId),
              eq(JobTypeUtils.STREAMING),
              eq(transformations)))
          .thenReturn(context);
      doNothing().when(context).onJobSubmitted();

      listener.onJobSubmitted(jobClient, null);
      verify(context, times(1)).onJobSubmitted();
      verify(tracker, times(1)).startTracking(context);
    }
  }

  @Test
  @SneakyThrows
  void testOnJobSubmittedViaConfiguration() {

    Configuration configuration = new Configuration();
    String customJobName = "testjob";
    configuration.setString("execution.job-listener.openlineage.job-name", customJobName);

    String customNamespace = "customized_namespace";
    configuration.setString("execution.job-listener.openlineage.namespace", customNamespace);

    StreamExecutionEnvironment streamExecutionEnvironment =
        new StreamExecutionEnvironment(configuration);

    FieldUtils.writeField(
        FieldUtils.getField(StreamExecutionEnvironment.class, "transformations", true),
        streamExecutionEnvironment,
        transformations,
        true);

    doNothing().when(tracker).startTracking(context);

    try (MockedStatic<FlinkExecutionContextFactory> contextFactory =
            mockStatic(FlinkExecutionContextFactory.class);
        MockedStatic<OpenLineageContinousJobTrackerFactory> jobtrackerFactory =
            mockStatic(OpenLineageContinousJobTrackerFactory.class)) {
      when(FlinkExecutionContextFactory.getContext(
              eq((Configuration) streamExecutionEnvironment.getConfiguration()),
              eq(customNamespace),
              eq(customJobName),
              eq(jobId),
              eq(JobTypeUtils.STREAMING),
              eq(transformations)))
          .thenReturn(context);
      when(OpenLineageContinousJobTrackerFactory.getTracker(
              configuration, OPENLINEAGE_LISTENER_CONFIG_DURATION.defaultValue()))
          .thenReturn(tracker);
      doNothing().when(context).onJobSubmitted();

      listener =
          OpenLineageFlinkJobListener.builder()
              .executionEnvironment(streamExecutionEnvironment)
              .build();
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

    JobExecutionResult jobExecutionResult = mock(JobExecutionResult.class);
    when(jobExecutionResult.getJobID()).thenReturn(jobId);
    doNothing().when(context).onJobSubmitted();
    doNothing().when(tracker).startTracking(context);

    try (MockedStatic<FlinkExecutionContextFactory> contextFactory =
        mockStatic(FlinkExecutionContextFactory.class)) {
      when(FlinkExecutionContextFactory.getContext(
              eq(readableConfig),
              eq(jobNamespace),
              eq(jobName),
              eq(jobId),
              eq(JobTypeUtils.STREAMING),
              eq(transformations)))
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

    doNothing().when(tracker).startTracking(context);

    listener =
        OpenLineageFlinkJobListener.builder()
            .executionEnvironment(streamExecutionEnvironment)
            .jobTracker(tracker)
            .build();

    try (MockedStatic<FlinkExecutionContextFactory> contextFactory =
        mockStatic(FlinkExecutionContextFactory.class)) {
      when(FlinkExecutionContextFactory.getContext(
              eq(readableConfig),
              eq(DEFAULT_JOB_NAMESPACE),
              eq(StreamGraphGenerator.DEFAULT_STREAMING_JOB_NAME),
              eq(jobId),
              eq(JobTypeUtils.STREAMING),
              eq(transformations)))
          .thenReturn(context);
      doNothing().when(context).onJobSubmitted();

      listener.onJobSubmitted(jobClient, null);
      verify(context, times(1)).onJobSubmitted();
      verify(tracker, times(1)).startTracking(context);
    }
  }
}
