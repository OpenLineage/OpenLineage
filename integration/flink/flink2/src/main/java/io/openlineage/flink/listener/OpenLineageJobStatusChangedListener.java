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
import io.openlineage.flink.tracker.OpenLineageContinousJobTracker;
import io.openlineage.flink.util.JobStatusUtil;
import io.openlineage.flink.visitor.Flink2VisitorFactory;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.core.execution.DefaultJobExecutionStatusEvent;
import org.apache.flink.core.execution.JobStatusChangedEvent;
import org.apache.flink.core.execution.JobStatusChangedListener;
import org.apache.flink.core.execution.JobStatusChangedListenerFactory.Context;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.streaming.runtime.execution.JobCreatedEvent;

@Slf4j
public class OpenLineageJobStatusChangedListener implements JobStatusChangedListener {
  public static final String DEFAULT_NAMESPACE = "flink-jobs";
  public static final String FLINK_JOB_FACET_KEY = "flink_job";
  private final OpenLineageContext context;
  private final LineageGraphConverter graphConverter;
  private final boolean isDetachedSubmission;
  private OpenLineageContinousJobTracker tracker;
  private boolean jobTrackingStarted = false;

  public OpenLineageJobStatusChangedListener(Context context, Flink2VisitorFactory visitorFactory) {
    this.isDetachedSubmission = !context.getConfiguration().get(DeploymentOptions.ATTACHED);
    this.context =
        OpenLineageContextFactory.fromConfig(FlinkConfigParser.parse(context.getConfiguration()))
            .build();
    log.info(
        "Creating OpenLineageJobStatusChangedListener with OpenLineageContext: {}", this.context);

    String jobsApiUrl =
        String.format(
            "http://%s:%s/jobs",
            Optional.ofNullable(context.getConfiguration().get(RestOptions.ADDRESS))
                .orElse("localhost"),
            context.getConfiguration().get(RestOptions.PORT));
    tracker =
        new OpenLineageContinousJobTracker(
            Duration.ofSeconds(this.context.getConfig().getTrackingIntervalInSeconds()),
            jobsApiUrl);
    graphConverter = new LineageGraphConverter(this.context, visitorFactory);
  }

  @VisibleForTesting
  OpenLineageJobStatusChangedListener(
      OpenLineageContext context,
      Flink2VisitorFactory visitorFactory,
      OpenLineageContinousJobTracker tracker,
      boolean isDetachedSubmission) {
    this.context = context;
    this.tracker = tracker;
    this.isDetachedSubmission = isDetachedSubmission;
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
    try {
      RunEvent startEvent = graphConverter.convert(event.lineageGraph(), EventType.START);
      // Only used when job is submitted in --detached mode
      if (isDetachedSubmission) {
        context.getEventEmitter().emit(startEvent, getDetachedStartEventEmitTimeout());
      } else {
        context.getEventEmitter().emit(startEvent);
      }
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
      startTrackingIfNeeded();
      return;
    }

    if (!event.newStatus().isGloballyTerminalState()) {
      log.debug("Skipping non-terminal status event: {}", event);
      return;
    }

    OpenLineage openLineage = context.getOpenLineage();
    RunEvent runEvent =
        commonEventBuilder()
            .eventType(JobStatusUtil.fromJobStatus(event.newStatus()))
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

    if (event.newStatus().isGloballyTerminalState() && tracker != null) {
      tracker.stopTracking();
      jobTrackingStarted = false;
    }
  }

  private boolean hasFlinkJobId() {
    return context.getJobId() != null && context.getJobId().getFlinkJobId() != null;
  }

  private void startTrackingIfNeeded() {
    if (!jobTrackingStarted && tracker != null) {
      tracker.startTracking(context, this::onJobCheckpoint);
      jobTrackingStarted = true;
    }
  }

  private Duration getDetachedStartEventEmitTimeout() {
    return Duration.ofSeconds(context.getConfig().getDetachedStartEventEmitTimeoutInSeconds());
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
    if (event.jobId() != null) {
      context.setRunUuidFromFlinkJobId(event.jobId());
    }
  }
}
