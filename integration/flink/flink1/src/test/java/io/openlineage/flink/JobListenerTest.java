/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink;

import static io.openlineage.flink.OpenLineageFlinkJobListener.DEFAULT_JOB_NAMESPACE;
import static io.openlineage.flink.OpenLineageFlinkJobListener.OPENLINEAGE_LISTENER_CONFIG_DURATION;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.openlineage.flink.api.OpenLineageContext;
import io.openlineage.flink.api.OpenLineageContext.JobIdentifier;
import io.openlineage.flink.tracker.OpenLineageContinousJobTracker;
import io.openlineage.flink.tracker.OpenLineageContinousJobTrackerFactory;
import io.openlineage.flink.utils.JobTypeUtils;
import io.openlineage.flink.visitor.lifecycle.FlinkExecutionContext;
import io.openlineage.flink.visitor.lifecycle.FlinkExecutionContextFactory;
import java.util.ArrayList;
import java.util.List;
import lombok.SneakyThrows;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

class JobListenerTest {

  JobClient jobClient = mock(JobClient.class);
  JobIdentifier jobId =
      JobIdentifier.builder()
          .jobNamespace("some-job-namespace")
          .jobName("some-job-name")
          .flinkJobId(new JobID(1, 2))
          .build();
  List<Transformation<?>> transformations = new ArrayList<>();
  OpenLineageFlinkJobListener listener;
  FlinkExecutionContext context = mock(FlinkExecutionContext.class);
  OpenLineageContext openLineageContext = mock(OpenLineageContext.class);
  OpenLineageContinousJobTracker tracker = mock(OpenLineageContinousJobTracker.class);
  Configuration readableConfig = new Configuration();

  @BeforeEach
  @SneakyThrows
  public void setup() {
    transformations.add(mock(Transformation.class));
    when(jobClient.getJobID()).thenReturn(jobId.getFlinkJobId());
    when(context.getOlContext()).thenReturn(openLineageContext);
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

    doNothing().when(tracker).startTracking(openLineageContext, context::onJobCheckpoint);

    listener =
        OpenLineageFlinkJobListener.builder()
            .executionEnvironment(streamExecutionEnvironment)
            .jobTracker(tracker)
            .jobNamespace(jobId.getJobNamespace())
            .jobName(jobId.getJobName())
            .build();

    try (MockedStatic<FlinkExecutionContextFactory> contextFactory =
        mockStatic(FlinkExecutionContextFactory.class)) {
      when(FlinkExecutionContextFactory.getContext(
              eq(readableConfig), eq(jobId), eq(JobTypeUtils.STREAMING), eq(transformations)))
          .thenReturn(context);
      doNothing().when(context).onJobSubmitted();

      listener.onJobSubmitted(jobClient, null);
      verify(context, times(1)).onJobSubmitted();
      verify(tracker, times(1)).startTracking(eq(openLineageContext), any());
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

    jobId =
        JobIdentifier.builder()
            .jobNamespace(customNamespace)
            .jobName(customJobName)
            .flinkJobId(new JobID(1, 2))
            .build();

    StreamExecutionEnvironment streamExecutionEnvironment =
        new StreamExecutionEnvironment(configuration);

    FieldUtils.writeField(
        FieldUtils.getField(StreamExecutionEnvironment.class, "transformations", true),
        streamExecutionEnvironment,
        transformations,
        true);

    doNothing().when(context).onJobSubmitted();
    doNothing().when(tracker).startTracking(openLineageContext, context::onJobCheckpoint);

    try (MockedStatic<FlinkExecutionContextFactory> contextFactory =
            mockStatic(FlinkExecutionContextFactory.class);
        MockedStatic<OpenLineageContinousJobTrackerFactory> jobtrackerFactory =
            mockStatic(OpenLineageContinousJobTrackerFactory.class)) {
      when(FlinkExecutionContextFactory.getContext(
              eq((Configuration) streamExecutionEnvironment.getConfiguration()),
              argThat(
                  jobIdentifier ->
                      jobIdentifier.getJobNamespace().equals(customNamespace)
                          && jobIdentifier.getJobName().equals(customJobName)),
              eq(JobTypeUtils.STREAMING),
              eq(transformations)))
          .thenReturn(context);
      when(OpenLineageContinousJobTrackerFactory.getTracker(
              configuration, OPENLINEAGE_LISTENER_CONFIG_DURATION.defaultValue()))
          .thenReturn(tracker);

      listener =
          OpenLineageFlinkJobListener.builder()
              .executionEnvironment(streamExecutionEnvironment)
              .build();
      listener.onJobSubmitted(jobClient, null);
      verify(context, times(1)).onJobSubmitted();
      verify(tracker, times(1)).startTracking(eq(openLineageContext), any());
    }
  }

  @SuppressWarnings("PMD")
  @Test
  @SneakyThrows
  void testOnJobExecutedSuccessfully() {
    StreamExecutionEnvironment streamExecutionEnvironment =
        new StreamExecutionEnvironment(readableConfig);
    FieldUtils.writeField(
        FieldUtils.getField(StreamExecutionEnvironment.class, "transformations", true),
        streamExecutionEnvironment,
        transformations,
        true);

    JobExecutionResult jobExecutionResult = mock(JobExecutionResult.class);
    when(jobExecutionResult.getJobID()).thenReturn(jobId.getFlinkJobId());
    doNothing().when(context).onJobSubmitted();
    doNothing().when(context).onJobCompleted(jobExecutionResult);
    doNothing().when(context).close();

    doNothing().when(tracker).startTracking(openLineageContext, context::onJobCheckpoint);

    listener =
        OpenLineageFlinkJobListener.builder()
            .executionEnvironment(streamExecutionEnvironment)
            .jobTracker(tracker)
            .jobNamespace(jobId.getJobNamespace())
            .jobName(jobId.getJobName())
            .build();

    try (MockedStatic<FlinkExecutionContextFactory> contextFactory =
        mockStatic(FlinkExecutionContextFactory.class)) {
      when(FlinkExecutionContextFactory.getContext(
              eq(readableConfig), eq(jobId), eq(JobTypeUtils.STREAMING), eq(transformations)))
          .thenReturn(context);

      listener.onJobSubmitted(jobClient, null);
      listener.onJobExecuted(jobExecutionResult, null);

      verify(tracker, times(1)).stopTracking();
      verify(context, times(1)).onJobCompleted(jobExecutionResult);
      // explicitly close context to send all pending events
      verify(context, times(1)).close();
    }
  }

  @SuppressWarnings("PMD")
  @Test
  @SneakyThrows
  void testOnJobExecutedFailure() {
    StreamExecutionEnvironment streamExecutionEnvironment =
        new StreamExecutionEnvironment(readableConfig);
    FieldUtils.writeField(
        FieldUtils.getField(StreamExecutionEnvironment.class, "transformations", true),
        streamExecutionEnvironment,
        transformations,
        true);

    JobExecutionResult jobExecutionResult = mock(JobExecutionResult.class);
    Throwable error = new Exception();
    when(jobExecutionResult.getJobID()).thenReturn(jobId.getFlinkJobId());
    doNothing().when(context).onJobSubmitted();
    doNothing().when(context).onJobFailed(error);
    doNothing().when(context).close();

    doNothing().when(tracker).startTracking(openLineageContext, context::onJobCheckpoint);

    listener =
        OpenLineageFlinkJobListener.builder()
            .executionEnvironment(streamExecutionEnvironment)
            .jobTracker(tracker)
            .jobNamespace(jobId.getJobNamespace())
            .jobName(jobId.getJobName())
            .build();

    try (MockedStatic<FlinkExecutionContextFactory> contextFactory =
        mockStatic(FlinkExecutionContextFactory.class)) {
      when(FlinkExecutionContextFactory.getContext(
              eq(readableConfig), eq(jobId), eq(JobTypeUtils.STREAMING), eq(transformations)))
          .thenReturn(context);

      listener.onJobSubmitted(jobClient, null);
      listener.onJobExecuted(null, error);

      verify(tracker, times(1)).stopTracking();
      verify(context, times(1)).onJobFailed(error);
      // explicitly close context to send all pending events
      verify(context, times(1)).close();
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

    doNothing().when(tracker).startTracking(openLineageContext, context::onJobCheckpoint);

    listener =
        OpenLineageFlinkJobListener.builder()
            .executionEnvironment(streamExecutionEnvironment)
            .jobTracker(tracker)
            .build();

    try (MockedStatic<FlinkExecutionContextFactory> contextFactory =
        mockStatic(FlinkExecutionContextFactory.class)) {
      when(FlinkExecutionContextFactory.getContext(
              eq(readableConfig),
              argThat(
                  jobIdentifier -> jobIdentifier.getJobNamespace().equals(DEFAULT_JOB_NAMESPACE)),
              eq(JobTypeUtils.STREAMING),
              eq(transformations)))
          .thenReturn(context);
      doNothing().when(context).onJobSubmitted();

      listener.onJobSubmitted(jobClient, null);
      verify(context, times(1)).onJobSubmitted();
      verify(tracker, times(1)).startTracking(eq(openLineageContext), any());
    }
  }
}
