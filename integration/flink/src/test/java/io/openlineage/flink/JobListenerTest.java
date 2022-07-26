/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.openlineage.flink.agent.lifecycle.FlinkExecutionContext;
import io.openlineage.flink.agent.lifecycle.FlinkExecutionContextFactory;
import io.openlineage.flink.tracker.OpenLineageContinousJobTracker;
import java.util.ArrayList;
import java.util.HashMap;
import lombok.SneakyThrows;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

public class JobListenerTest {

  JobClient jobClient = mock(JobClient.class);
  JobID jobId = mock(JobID.class);
  ArrayList<Transformation<?>> transformations = new ArrayList<>();
  OpenLineageFlinkJobListener listener;
  FlinkExecutionContext context = mock(FlinkExecutionContext.class);
  ReadableConfig readableConfig = mock(ReadableConfig.class);

  @BeforeEach
  @SneakyThrows
  public void setup() {
    transformations.add(mock(Transformation.class));
    when(jobClient.getJobID()).thenReturn(jobId);
  }

  @Test
  @SneakyThrows
  public void testOnJobSubmitted() {
    StreamExecutionEnvironment streamExecutionEnvironment = new StreamExecutionEnvironment();
    FieldUtils.writeField(
        FieldUtils.getField(StreamExecutionEnvironment.class, "transformations", true),
        streamExecutionEnvironment,
        transformations,
        true);

    OpenLineageContinousJobTracker tracker = mock(OpenLineageContinousJobTracker.class);
    doNothing().when(tracker).startTracking(context);

    listener = new OpenLineageFlinkJobListener(streamExecutionEnvironment, tracker);
    try (MockedStatic<FlinkExecutionContextFactory> contextFactory =
        mockStatic(FlinkExecutionContextFactory.class)) {
      when(FlinkExecutionContextFactory.getContext(eq(jobId), eq(transformations)))
          .thenReturn(context);
      doNothing().when(context).onJobSubmitted();

      listener.onJobSubmitted(jobClient, null);
      verify(context, times(1)).onJobSubmitted();
      verify(tracker, times(1)).startTracking(context);
    }
  }

  @Test
  @SneakyThrows
  public void testOnJobExecuted() {
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
      when(FlinkExecutionContextFactory.getContext(eq(jobId), any())).thenReturn(context);
      doNothing().when(context).onJobSubmitted();

      listener = new OpenLineageFlinkJobListener(streamExecutionEnvironment, tracker);
      listener.onJobSubmitted(jobClient, null);
      listener.onJobExecuted(jobExecutionResult, null);
    }
  }
}
