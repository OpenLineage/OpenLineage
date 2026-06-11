/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.listener;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.openlineage.client.OpenLineage.RunEvent;
import io.openlineage.client.OpenLineage.RunEvent.EventType;
import io.openlineage.client.OpenLineageClientUtils;
import io.openlineage.flink.api.OpenLineageContext;
import io.openlineage.flink.api.OpenLineageContextFactory;
import io.openlineage.flink.client.EventEmitter;
import io.openlineage.flink.config.FlinkConfigParser;
import io.openlineage.flink.tracker.OpenLineageContinousJobTracker;
import io.openlineage.flink.visitor.Flink2VisitorFactory;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
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

class OpenLineageDetachedJobStatusChangedListenerTest {
  private static final Instant JOB_START_TIME = Instant.parse("2026-01-01T00:00:00Z");

  Context context = mock(Context.class, RETURNS_DEEP_STUBS);
  Flink2VisitorFactory factory = mock(Flink2VisitorFactory.class);
  OpenLineageDetachedJobStatusChangedListener listener;
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
  void testDetachedJobCreatedEventUsesBoundedEmit() {
    EventEmitter eventEmitter = mock(EventEmitter.class);
    Configuration configuration =
        Configuration.fromMap(
            Map.of(
                "openlineage.transport.type",
                "file",
                "openlineage.transport.location",
                eventFileLocation,
                "openlineage.flink.detachedStartEventEmitTimeoutInSeconds",
                "7"));
    OpenLineageContext openLineageContext =
        OpenLineageContextFactory.fromConfig(FlinkConfigParser.parse(configuration))
            .eventEmitter(eventEmitter)
            .build();
    listener =
        new OpenLineageDetachedJobStatusChangedListener(
            openLineageContext, factory, null, jobId -> Optional.of(JOB_START_TIME));
    JobCreatedEvent createdEvent = mock(JobCreatedEvent.class);
    when(createdEvent.jobId()).thenReturn(new JobID(1, 2));
    when(createdEvent.jobName()).thenReturn("event-job-name");

    listener.onEvent(createdEvent);

    verify(eventEmitter, times(1)).emit(any(RunEvent.class), eq(Duration.ofSeconds(7)));
    verify(eventEmitter, times(0)).emit(any(RunEvent.class));
  }

  @Test
  void testDetachedJobCreatedEventSkipsStartWhenRunUuidCannotBeInitialized() {
    EventEmitter eventEmitter = mock(EventEmitter.class);
    OpenLineageContext openLineageContext =
        OpenLineageContextFactory.fromConfig(FlinkConfigParser.parse(context.getConfiguration()))
            .eventEmitter(eventEmitter)
            .build();
    listener =
        new OpenLineageDetachedJobStatusChangedListener(
            openLineageContext, factory, null, jobId -> Optional.empty());
    JobCreatedEvent createdEvent = mock(JobCreatedEvent.class);
    when(createdEvent.jobId()).thenReturn(new JobID(1, 2));
    when(createdEvent.jobName()).thenReturn("event-job-name");

    listener.onEvent(createdEvent);

    verify(eventEmitter, times(0)).emit(any(RunEvent.class), any(Duration.class));
    verify(eventEmitter, times(0)).emit(any(RunEvent.class));
  }

  @Test
  @SneakyThrows
  void testRuntimeStatusEventStartsTrackingWithoutImmediateRunningEvent() {
    OpenLineageContext openLineageContext =
        OpenLineageContextFactory.fromConfig(FlinkConfigParser.parse(context.getConfiguration()))
            .build();
    OpenLineageContinousJobTracker tracker = mock(OpenLineageContinousJobTracker.class);
    listener =
        new OpenLineageDetachedJobStatusChangedListener(
            openLineageContext, factory, tracker, jobId -> Optional.of(JOB_START_TIME));
    JobID jobId = new JobID(1, 2);

    listener.onEvent(
        new DefaultJobExecutionStatusEvent(
            jobId, "runtime-job-name", JobStatus.CREATED, JobStatus.RUNNING, null));

    assertThat(Files.exists(Path.of(eventFileLocation))).isFalse();
    verify(tracker, times(1)).startTracking(any(OpenLineageContext.class), any());
  }

  @Test
  @SneakyThrows
  void testRuntimeStatusEventStartsTrackingAfterRunUuidIsInitialized() {
    OpenLineageContext openLineageContext =
        OpenLineageContextFactory.fromConfig(FlinkConfigParser.parse(context.getConfiguration()))
            .build();
    OpenLineageContinousJobTracker tracker = mock(OpenLineageContinousJobTracker.class);
    AtomicReference<Runnable> runUuidInitialization = new AtomicReference<>();
    listener =
        new OpenLineageDetachedJobStatusChangedListener(
            openLineageContext,
            factory,
            tracker,
            jobId -> Optional.of(JOB_START_TIME),
            runUuidInitialization::set);
    JobID jobId = new JobID(1, 2);

    listener.onEvent(
        new DefaultJobExecutionStatusEvent(
            jobId, "runtime-job-name", JobStatus.CREATED, JobStatus.RUNNING, null));

    verify(tracker, times(0)).startTracking(any(OpenLineageContext.class), any());
    assertThat(Files.exists(Path.of(eventFileLocation))).isFalse();
    assertThat(runUuidInitialization.get()).isNotNull();

    runUuidInitialization.get().run();

    assertThat(Files.exists(Path.of(eventFileLocation))).isFalse();
    verify(tracker, times(1)).startTracking(any(OpenLineageContext.class), any());
  }

  @Test
  void testRuntimeStatusEventSkipsTrackingWhenRunUuidCannotBeInitialized() {
    OpenLineageContext openLineageContext =
        OpenLineageContextFactory.fromConfig(FlinkConfigParser.parse(context.getConfiguration()))
            .build();
    OpenLineageContinousJobTracker tracker = mock(OpenLineageContinousJobTracker.class);
    listener =
        new OpenLineageDetachedJobStatusChangedListener(
            openLineageContext, factory, tracker, jobId -> Optional.empty());
    JobID jobId = new JobID(1, 2);

    listener.onEvent(
        new DefaultJobExecutionStatusEvent(
            jobId, "runtime-job-name", JobStatus.CREATED, JobStatus.RUNNING, null));

    assertThat(Files.exists(Path.of(eventFileLocation))).isFalse();
    verify(tracker, times(0)).startTracking(any(OpenLineageContext.class), any());
  }

  @Test
  @SneakyThrows
  void testRuntimeTerminalEventLoadsJobIdAndEmitsWithDeterministicRunId() {
    OpenLineageContext openLineageContext =
        OpenLineageContextFactory.fromConfig(FlinkConfigParser.parse(context.getConfiguration()))
            .build();
    OpenLineageContinousJobTracker tracker = mock(OpenLineageContinousJobTracker.class);
    listener =
        new OpenLineageDetachedJobStatusChangedListener(
            openLineageContext, factory, tracker, jobId -> Optional.of(JOB_START_TIME));
    JobID jobId = new JobID(1, 2);

    listener.onEvent(
        new DefaultJobExecutionStatusEvent(
            jobId, "runtime-job-name", JobStatus.RUNNING, JobStatus.FAILED, null));

    List<RunEvent> eventsEmitted =
        Files.readAllLines(Path.of(eventFileLocation)).stream()
            .map(OpenLineageClientUtils::runEventFromJson)
            .collect(Collectors.toList());

    assertThat(eventsEmitted).hasSize(1);
    assertThat(eventsEmitted.get(0).getJob().getNamespace()).isEqualTo("flink-jobs");
    assertThat(eventsEmitted.get(0).getJob().getName()).isEqualTo("runtime-job-name");
    assertThat(eventsEmitted.get(0).getRun().getRunId()).isEqualTo(openLineageContext.getRunUuid());
    assertThat(eventsEmitted.get(0).getRun().getRunId().version()).isEqualTo(7);
    assertThat(eventsEmitted.get(0).getEventType()).isEqualTo(EventType.FAIL);
    verify(tracker, times(1)).stopTracking();
  }

  @Test
  void testRuntimeTerminalEventSkipsEmitWhenRunUuidCannotBeInitialized() {
    OpenLineageContext openLineageContext =
        OpenLineageContextFactory.fromConfig(FlinkConfigParser.parse(context.getConfiguration()))
            .build();
    OpenLineageContinousJobTracker tracker = mock(OpenLineageContinousJobTracker.class);
    listener =
        new OpenLineageDetachedJobStatusChangedListener(
            openLineageContext, factory, tracker, jobId -> Optional.empty());
    JobID jobId = new JobID(1, 2);

    listener.onEvent(
        new DefaultJobExecutionStatusEvent(
            jobId, "runtime-job-name", JobStatus.RUNNING, JobStatus.FAILED, null));

    assertThat(Files.exists(Path.of(eventFileLocation))).isFalse();
    verify(tracker, times(0)).stopTracking();
  }

  @Test
  @SneakyThrows
  void testRuntimeStatusEventDoesNotStartTrackingAfterTerminalStatusObserved() {
    OpenLineageContext openLineageContext =
        OpenLineageContextFactory.fromConfig(FlinkConfigParser.parse(context.getConfiguration()))
            .build();
    OpenLineageContinousJobTracker tracker = mock(OpenLineageContinousJobTracker.class);
    AtomicReference<Runnable> runUuidInitialization = new AtomicReference<>();
    listener =
        new OpenLineageDetachedJobStatusChangedListener(
            openLineageContext,
            factory,
            tracker,
            jobId -> Optional.of(JOB_START_TIME),
            runUuidInitialization::set);
    JobID jobId = new JobID(1, 2);

    listener.onEvent(
        new DefaultJobExecutionStatusEvent(
            jobId, "runtime-job-name", JobStatus.CREATED, JobStatus.RUNNING, null));
    listener.onEvent(
        new DefaultJobExecutionStatusEvent(
            jobId, "runtime-job-name", JobStatus.RUNNING, JobStatus.FAILED, null));

    runUuidInitialization.get().run();

    List<RunEvent> eventsEmitted =
        Files.readAllLines(Path.of(eventFileLocation)).stream()
            .map(OpenLineageClientUtils::runEventFromJson)
            .collect(Collectors.toList());

    assertThat(eventsEmitted).hasSize(1);
    assertThat(eventsEmitted.get(0).getEventType()).isEqualTo(EventType.FAIL);
    verify(tracker, times(0)).startTracking(any(OpenLineageContext.class), any());
  }
}
