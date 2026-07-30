/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.listener;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.RunEvent;
import io.openlineage.client.OpenLineage.RunEvent.EventType;
import io.openlineage.client.OpenLineage.RunEventBuilder;
import io.openlineage.client.OpenLineageClientUtils;
import io.openlineage.flink.api.OpenLineageContext;
import io.openlineage.flink.api.OpenLineageContext.JobIdentifier;
import io.openlineage.flink.api.OpenLineageContextFactory;
import io.openlineage.flink.client.CheckpointFacet;
import io.openlineage.flink.client.Versions;
import io.openlineage.flink.config.FlinkConfigParser;
import io.openlineage.flink.config.FlinkOpenLineageConfig;
import io.openlineage.flink.converter.LineageGraphConverter;
import io.openlineage.flink.facets.FlinkJobDetailsFacet;
import io.openlineage.flink.tracker.FlinkRestApiClient;
import io.openlineage.flink.tracker.OpenLineageContinousJobTracker;
import io.openlineage.flink.util.JobStatusUtil;
import io.openlineage.flink.visitor.Flink2VisitorFactory;
import java.time.Duration;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.core.execution.DefaultJobExecutionStatusEvent;
import org.apache.flink.core.execution.JobStatusChangedEvent;
import org.apache.flink.core.execution.JobStatusChangedListener;
import org.apache.flink.core.execution.JobStatusChangedListenerFactory.Context;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.streaming.runtime.execution.JobCreatedEvent;

/**
 * Job status listener for Flink deployments where the submitting client and JobManager observe
 * different parts of the job lifecycle.
 *
 * <p>This is especially useful when a job is submitted from a separate machine or process, and the
 * JobManager later observes the runtime status changes. In detached/session-style deployments, the
 * client-side listener can emit the {@code START} event and then exit, while later runtime status
 * events are observed by the JobManager listener in another JVM.
 *
 * <p>This listener reconstructs the same OpenLineage run id on the JobManager side from the Flink
 * job id and the job start time read from Flink's REST API, then uses that run id for terminal and
 * checkpoint-derived events.
 *
 * <p>Enable this listener with {@code openlineage.flink.enableDetachedJobTracking=true}. The
 * regular {@link OpenLineageJobStatusChangedListener} remains the default for deployments where all
 * lifecycle events are handled by the same listener instance.
 *
 * <p>The initial JobManager {@code RUNNING} status event is intentionally not emitted as an
 * OpenLineage {@code RUNNING} event because it can race ahead of the client-side {@code START}
 * event. {@code RUNNING} events are emitted later from checkpoint tracking.
 */
@Slf4j
public class OpenLineageDetachedJobStatusChangedListener implements JobStatusChangedListener {
  public static final String DEFAULT_NAMESPACE = "flink-jobs";
  public static final String FLINK_JOB_FACET_KEY = "flink_job";
  private final OpenLineageContext context;
  private final LineageGraphConverter graphConverter;
  private final Executor ioExecutor;
  private final Function<JobID, Optional<Instant>> jobStartTimeProvider;
  private OpenLineageContinousJobTracker tracker;
  private volatile boolean jobTrackingStarted = false;
  private volatile boolean terminalStatusObserved = false;
  private volatile CompletableFuture<Boolean> runUuidInitialization;

  public OpenLineageDetachedJobStatusChangedListener(
      Context context, Flink2VisitorFactory visitorFactory) {
    this.context =
        OpenLineageContextFactory.fromConfig(FlinkConfigParser.parse(context.getConfiguration()))
            .build();
    log.info(
        "Creating OpenLineageDetachedJobStatusChangedListener with OpenLineageContext: {}",
        this.context);

    String jobsApiUrl =
        String.format(
            "http://%s:%s/jobs",
            Optional.ofNullable(context.getConfiguration().get(RestOptions.ADDRESS))
                .orElse("localhost"),
            context.getConfiguration().get(RestOptions.PORT));
    this.ioExecutor = context.getIOExecutor();
    this.jobStartTimeProvider = new FlinkRestApiClient(jobsApiUrl)::getJobStartTime;
    tracker =
        new OpenLineageContinousJobTracker(
            Duration.ofSeconds(this.context.getConfig().getTrackingIntervalInSeconds()),
            jobsApiUrl);
    graphConverter = new LineageGraphConverter(this.context, visitorFactory);
  }

  @VisibleForTesting
  OpenLineageDetachedJobStatusChangedListener(
      OpenLineageContext context,
      Flink2VisitorFactory visitorFactory,
      OpenLineageContinousJobTracker tracker,
      Function<JobID, Optional<Instant>> jobStartTimeProvider) {
    this(context, visitorFactory, tracker, jobStartTimeProvider, Runnable::run);
  }

  @VisibleForTesting
  OpenLineageDetachedJobStatusChangedListener(
      OpenLineageContext context,
      Flink2VisitorFactory visitorFactory,
      OpenLineageContinousJobTracker tracker,
      Function<JobID, Optional<Instant>> jobStartTimeProvider,
      Executor ioExecutor) {
    this.context = context;
    this.tracker = tracker;
    this.ioExecutor = ioExecutor;
    this.jobStartTimeProvider = jobStartTimeProvider;
    graphConverter = new LineageGraphConverter(this.context, visitorFactory);
  }

  @Override
  @SuppressWarnings("PMD.AvoidCatchingThrowable")
  public void onEvent(JobStatusChangedEvent event) {
    context
        .getCircuitBreaker()
        .run(
            () -> {
              if (event instanceof JobCreatedEvent) {
                log.debug("triggered onEvent for JobCreatedEvent: {}", event);
                onJobCreatedEvent((JobCreatedEvent) event);
              } else if (event instanceof DefaultJobExecutionStatusEvent) {
                log.debug("triggered onEvent for DefaultJobExecutionStatusEvent: {}", event);
                onDefaultJobExecutionStatusEvent((DefaultJobExecutionStatusEvent) event);
              } else {
                log.warn("Unsupported event: {}", event.getClass());
              }
              return null;
            });
  }

  private void onJobCreatedEvent(JobCreatedEvent event) {
    loadJobId(event);
    if (!setDeterministicRunUuid(event)) {
      log.warn("Skipping START event because runUuid could not be initialized: {}", event);
      return;
    }

    try {
      RunEvent startEvent = graphConverter.convert(event.lineageGraph(), EventType.START);
      context.getEventEmitter().emit(startEvent, getDetachedStartEventEmitTimeout());
    } catch (Exception e) {
      log.error("Triggering event caused an exception", e);
    }
  }

  private void onJobCheckpoint(CheckpointFacet checkpointFacet) {
    log.info("Emitting checkpoint event: {}", checkpointFacet);
    OpenLineage openLineage = context.getOpenLineage();
    RunEvent runEvent =
        commonEventBuilder()
            .eventType(EventType.RUNNING)
            .run(
                openLineage
                    .newRunBuilder()
                    .runId(context.getRunUuid())
                    .facets(
                        openLineage
                            .newRunFacetsBuilder()
                            .processing_engine(buildProcessingEngineFacet(openLineage))
                            .put("checkpoints", checkpointFacet)
                            .put(FLINK_JOB_FACET_KEY, buildJobDetailsFacet())
                            .build())
                    .build())
            .build();

    if (log.isDebugEnabled()) {
      log.debug("Emitting checkpoint event: {}", OpenLineageClientUtils.toJson(runEvent));
    }
    context.getEventEmitter().emit(runEvent);
  }

  private void onDefaultJobExecutionStatusEvent(DefaultJobExecutionStatusEvent event) {
    if (!hasFlinkJobId()) {
      loadJobId(event);
    }

    if (!hasFlinkJobId()) {
      log.warn("JobId is not set, skipping event: {}", event);
      return;
    }

    if (event.newStatus() == JobStatus.RUNNING) {
      onRunUuidInitialized(event, this::startTrackingIfNeeded);
      return;
    }

    if (!event.newStatus().isGloballyTerminalState()) {
      log.debug("Skipping non-terminal status event: {}", event);
      return;
    }

    // Avoid starting checkpoint tracking from a delayed RUNNING callback after a terminal status.
    terminalStatusObserved = true;
    onRunUuidInitialized(event, () -> emitTerminalEvent(event));
  }

  private void onRunUuidInitialized(JobStatusChangedEvent event, Runnable action) {
    initializeRunUuidAsync(event)
        .thenAccept(
            initialized -> {
              if (initialized) {
                action.run();
              }
            });
  }

  private void emitTerminalEvent(DefaultJobExecutionStatusEvent event) {
    emitStatusEvent(JobStatusUtil.fromJobStatus(event.newStatus()));

    if (tracker != null) {
      tracker.stopTracking();
      jobTrackingStarted = false;
    }
  }

  private boolean hasFlinkJobId() {
    return context.getJobId() != null && context.getJobId().getFlinkJobId() != null;
  }

  private void emitStatusEvent(EventType eventType) {
    OpenLineage openLineage = context.getOpenLineage();
    RunEvent runEvent =
        commonEventBuilder()
            .eventType(eventType)
            .run(
                openLineage
                    .newRunBuilder()
                    .runId(context.getRunUuid())
                    .facets(
                        openLineage
                            .newRunFacetsBuilder()
                            .processing_engine(buildProcessingEngineFacet(openLineage))
                            .put(FLINK_JOB_FACET_KEY, buildJobDetailsFacet())
                            .build())
                    .build())
            .build();

    context.getEventEmitter().emit(runEvent);
  }

  private void startTrackingIfNeeded() {
    if (context.getConfig().getDisableCheckpointTracking()) {
      log.info("Checkpoint tracking is disabled via disableCheckpointTracking config");
      return;
    }
    if (!jobTrackingStarted && !terminalStatusObserved && tracker != null) {
      tracker.startTracking(context, this::onJobCheckpoint);
      jobTrackingStarted = true;
    }
  }

  private Duration getDetachedStartEventEmitTimeout() {
    return Duration.ofSeconds(context.getConfig().getDetachedStartEventEmitTimeoutInSeconds());
  }

  // Async because JM's REST API deadlocks when called synchronously during state transitions.
  private synchronized CompletableFuture<Boolean> initializeRunUuidAsync(
      JobStatusChangedEvent event) {
    if (event.jobId() == null) {
      return CompletableFuture.completedFuture(false);
    }

    if (runUuidInitialization == null) {
      runUuidInitialization =
          CompletableFuture.supplyAsync(() -> setDeterministicRunUuid(event), ioExecutor);
    }

    return runUuidInitialization;
  }

  private RunEventBuilder commonEventBuilder() {
    return context
        .getOpenLineage()
        .newRunEventBuilder()
        .eventTime(ZonedDateTime.now())
        .job(
            context
                .getOpenLineage()
                .newJobBuilder()
                .namespace(context.getJobId().getJobNamespace())
                .name(context.getJobId().getJobName())
                .build());
  }

  private OpenLineage.ProcessingEngineRunFacet buildProcessingEngineFacet(OpenLineage openLineage) {
    return openLineage
        .newProcessingEngineRunFacetBuilder()
        .name("flink")
        .version(EnvironmentInformation.getVersion())
        .openlineageAdapterVersion(Versions.getVersion())
        .build();
  }

  private FlinkJobDetailsFacet buildJobDetailsFacet() {
    JobIdentifier jobId = context.getJobId();
    if (jobId == null || jobId.getFlinkJobId() == null) {
      return null;
    }

    String flinkJobId = jobId.getFlinkJobId().toString();
    return new FlinkJobDetailsFacet(flinkJobId);
  }

  private void loadJobId(JobStatusChangedEvent event) {
    String jobName =
        Optional.ofNullable(context.getConfig())
            .map(FlinkOpenLineageConfig::getJobConfig)
            .map(j -> j.getName())
            .orElse(event.jobName());

    String jobNamespace =
        Optional.ofNullable(context.getConfig())
            .map(FlinkOpenLineageConfig::getJobConfig)
            .map(j -> j.getNamespace())
            .orElse(DEFAULT_NAMESPACE);

    JobIdentifier jobId =
        JobIdentifier.builder()
            .jobName(jobName)
            .jobNamespace(jobNamespace)
            .flinkJobId(event.jobId())
            .build();
    log.info("JobIdentifier with jobId: {}", jobId.getFlinkJobId());
    context.setJobId(jobId);
  }

  private boolean setDeterministicRunUuid(JobStatusChangedEvent event) {
    if (event.jobId() == null) {
      return false;
    }
    Optional<Instant> jobStartTime = jobStartTimeProvider.apply(event.jobId());
    if (jobStartTime.isPresent()) {
      context.setRunUuidFromFlinkJobId(event.jobId(), jobStartTime.get());
    }
    return jobStartTime.isPresent();
  }
}
