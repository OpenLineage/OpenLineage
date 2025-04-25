/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.listener;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.openlineage.client.OpenLineage.RunEvent;
import io.openlineage.client.OpenLineage.RunEvent.EventType;
import io.openlineage.client.OpenLineageClientUtils;
import io.openlineage.client.circuitBreaker.CircuitBreaker;
import io.openlineage.flink.api.OpenLineageContext;
import io.openlineage.flink.visitor.Flink2VisitorFactory;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.DefaultJobExecutionStatusEvent;
import org.apache.flink.core.execution.JobStatusChangedListenerFactory.Context;
import org.apache.flink.streaming.runtime.execution.JobCreatedEvent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class OpenLineageJobStatusChangedListenerTest {
  Context context = mock(Context.class, RETURNS_DEEP_STUBS);
  Flink2VisitorFactory factory = mock(Flink2VisitorFactory.class);
  OpenLineageJobStatusChangedListener listener;
  String eventFileLocation;

  @BeforeEach
  @SneakyThrows
  void setup() {
    if (!Files.isDirectory(Path.of("build/test_events"))) {
      Files.createDirectory(Path.of("build/test_events"));
    }
    eventFileLocation = "build/test_events/events_" + UUID.randomUUID();

    Configuration configuration =
        Configuration.fromMap(
            Map.of(
                "openlineage.transport.type",
                "file",
                "openlineage.transport.location",
                eventFileLocation));

    when(context.getConfiguration()).thenReturn(configuration);
  }

  @AfterEach
  void cleanup() throws IOException {
    Files.deleteIfExists(Path.of(eventFileLocation));
  }

  @Test
  @SneakyThrows
  void testOnEventForJobCreated() {
    listener = new OpenLineageJobStatusChangedListener(context, factory);
    JobCreatedEvent createdEvent = mock(JobCreatedEvent.class);
    when(createdEvent.jobName()).thenReturn("event-job-name");
    listener.onEvent(createdEvent);

    Path path = Path.of(eventFileLocation);
    assertThat(Files.exists(path)).isTrue();

    RunEvent event =
        Files.readAllLines(path).stream()
            .map(OpenLineageClientUtils::runEventFromJson)
            .collect(Collectors.toList())
            .get(0);

    assertThat(event.getJob().getNamespace()).isEqualTo("flink-jobs");
    assertThat(event.getJob().getName()).isEqualTo("event-job-name");
    assertThat(event.getRun().getRunId()).isNotNull();
    assertThat(event.getEventType()).isEqualTo(EventType.START);
  }

  @Test
  @SneakyThrows
  void testOnEventWithJobNameInConfig() {
    Configuration configuration =
        Configuration.fromMap(
            Map.of(
                "openlineage.transport.type",
                "file",
                "openlineage.transport.location",
                eventFileLocation,
                "openlineage.job.name",
                "config-job-name"));

    when(context.getConfiguration()).thenReturn(configuration);

    listener = new OpenLineageJobStatusChangedListener(context, factory);
    JobCreatedEvent createdEvent = mock(JobCreatedEvent.class);
    when(createdEvent.jobName()).thenReturn("event-job-name");
    listener.onEvent(createdEvent);

    Path path = Path.of(eventFileLocation);
    assertThat(Files.exists(path)).isTrue();

    RunEvent event =
        Files.readAllLines(path).stream()
            .map(OpenLineageClientUtils::runEventFromJson)
            .collect(Collectors.toList())
            .get(0);

    assertThat(event.getJob().getNamespace()).isEqualTo("flink-jobs");
    assertThat(event.getJob().getName()).isEqualTo("config-job-name");
    assertThat(event.getRun().getRunId()).isNotNull();
    assertThat(event.getEventType()).isEqualTo(EventType.START);
  }

  @Test
  @SneakyThrows
  void testOnEventWithJobNamespaceInConfig() {
    Configuration configuration =
        Configuration.fromMap(
            Map.of(
                "openlineage.transport.type",
                "file",
                "openlineage.transport.location",
                eventFileLocation,
                "openlineage.job.namespace",
                "flink://my.flink.domain:8081"));

    when(context.getConfiguration()).thenReturn(configuration);

    listener = new OpenLineageJobStatusChangedListener(context, factory);
    JobCreatedEvent createdEvent = mock(JobCreatedEvent.class);
    when(createdEvent.jobName()).thenReturn("event-job-name");
    listener.onEvent(createdEvent);

    Path path = Path.of(eventFileLocation);
    assertThat(Files.exists(path)).isTrue();

    RunEvent event =
        Files.readAllLines(path).stream()
            .map(OpenLineageClientUtils::runEventFromJson)
            .collect(Collectors.toList())
            .get(0);

    assertThat(event.getJob().getNamespace()).isEqualTo("flink://my.flink.domain:8081");
    assertThat(event.getJob().getName()).isEqualTo("event-job-name");
    assertThat(event.getRun().getRunId()).isNotNull();
    assertThat(event.getEventType()).isEqualTo(EventType.START);
  }

  @Test
  @SneakyThrows
  void testOnEventJobFinished() {
    listener = new OpenLineageJobStatusChangedListener(context, factory);

    // emit start event
    JobCreatedEvent createdEvent = mock(JobCreatedEvent.class);
    when(createdEvent.jobName()).thenReturn("event-job-name");
    listener.onEvent(createdEvent);

    // emit complete event
    DefaultJobExecutionStatusEvent statusEvent =
        new DefaultJobExecutionStatusEvent(
            mock(JobID.class),
            "jobName",
            JobStatus.RUNNING,
            JobStatus.FINISHED,
            mock(Throwable.class));
    listener.onEvent(statusEvent);

    Path path = Path.of(eventFileLocation);
    assertThat(Files.exists(path)).isTrue();

    List<RunEvent> eventsEmitted =
        Files.readAllLines(path).stream()
            .map(OpenLineageClientUtils::runEventFromJson)
            .collect(Collectors.toList());

    assertThat(eventsEmitted).hasSize(2);

    assertThat(eventsEmitted.get(0).getJob().getNamespace()).isEqualTo("flink-jobs");
    assertThat(eventsEmitted.get(0).getJob().getName()).isEqualTo("event-job-name");
    assertThat(eventsEmitted.get(0).getEventType()).isEqualTo(EventType.START);

    assertThat(eventsEmitted.get(1).getJob().getNamespace()).isEqualTo("flink-jobs");
    assertThat(eventsEmitted.get(1).getJob().getName()).isEqualTo("event-job-name");
    assertThat(eventsEmitted.get(1).getEventType()).isEqualTo(EventType.COMPLETE);
  }

  @Test
  @SneakyThrows
  void testOnEventJobFailed() {
    listener = new OpenLineageJobStatusChangedListener(context, factory);

    // emit start event
    JobCreatedEvent createdEvent = mock(JobCreatedEvent.class);
    when(createdEvent.jobName()).thenReturn("event-job-name");
    listener.onEvent(createdEvent);

    // emit fail event
    DefaultJobExecutionStatusEvent statusEvent =
        new DefaultJobExecutionStatusEvent(
            mock(JobID.class),
            "jobName",
            JobStatus.RUNNING,
            JobStatus.FAILED,
            mock(Throwable.class));
    listener.onEvent(statusEvent);

    List<RunEvent> eventsEmitted =
        Files.readAllLines(Path.of(eventFileLocation)).stream()
            .map(OpenLineageClientUtils::runEventFromJson)
            .collect(Collectors.toList());

    assertThat(eventsEmitted).hasSize(2);
    assertThat(eventsEmitted.get(0).getEventType()).isEqualTo(EventType.START);
    assertThat(eventsEmitted.get(1).getEventType()).isEqualTo(EventType.FAIL);
  }

  @Test
  void testCircuitBreaker() {
    OpenLineageContext openLineageContext = mock(OpenLineageContext.class);
    CircuitBreaker circuitBreaker = mock(CircuitBreaker.class);
    when(openLineageContext.getCircuitBreaker()).thenReturn(circuitBreaker);
    listener = new OpenLineageJobStatusChangedListener(openLineageContext, factory);
    listener.onEvent(mock(JobCreatedEvent.class));
    verify(circuitBreaker, times(1)).run(any(Callable.class));
  }
}
