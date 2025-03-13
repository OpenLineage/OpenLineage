/*
/* Copyright 2018-2025 contributors to the OpenLineage project
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
import io.openlineage.flink.config.FlinkConfigParser;
import io.openlineage.flink.config.FlinkOpenLineageConfig;
import io.openlineage.flink.converter.LineageGraphConverter;
import io.openlineage.flink.tracker.OpenLineageContinousJobTracker;
import io.openlineage.flink.util.JobStatusUtil;
import io.openlineage.flink.visitor.Flink2VisitorFactory;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.core.execution.DefaultJobExecutionStatusEvent;
import org.apache.flink.core.execution.JobStatusChangedEvent;
import org.apache.flink.core.execution.JobStatusChangedListener;
import org.apache.flink.core.execution.JobStatusChangedListenerFactory.Context;
import org.apache.flink.streaming.runtime.execution.JobCreatedEvent;

@Slf4j
public class OpenLineageJobStatusChangedListener implements JobStatusChangedListener {
  public static final String DEFAULT_NAMESPACE = "flink-jobs";
  private final OpenLineageContext context;
  private final LineageGraphConverter graphConverter;
  private OpenLineageContinousJobTracker tracker;

  public OpenLineageJobStatusChangedListener(Context context, Flink2VisitorFactory visitorFactory) {
    this.context =
        OpenLineageContextFactory.fromConfig(FlinkConfigParser.parse(context.getConfiguration()))
            .build();

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
      OpenLineageContext context, Flink2VisitorFactory visitorFactory) {
    this.context = context;
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
      context.getEventEmitter().emit(graphConverter.convert(event.lineageGraph(), EventType.START));
    } catch (Exception e) {
      log.error("Triggering event caused an exception", e);
    }
    tracker.startTracking(context, this::onJobCheckpoint);
  }

  private void onJobCheckpoint(CheckpointFacet checkpointFacet) {
    log.info("Emitting checkpoint event: {}", checkpointFacet);
    RunEvent runEvent =
        commonEventBuilder()
            .eventType(EventType.RUNNING)
            .run(
                new OpenLineage.RunBuilder()
                    .runId(context.getRunUuid())
                    .facets(
                        new OpenLineage.RunFacetsBuilder()
                            .put("checkpoints", checkpointFacet)
                            .build())
                    .build())
            .build();

    if (log.isDebugEnabled()) {
      log.debug("Emitting checkpoint event: {}", OpenLineageClientUtils.toJson(runEvent));
    }
    context.getEventEmitter().emit(runEvent);
  }

  private void onDefaultJobExecutionStatusEvent(DefaultJobExecutionStatusEvent event) {
    if (context.getJobId() == null) {
      log.warn("JobId is not set, skipping event: {}", event);
      return;
    }

    RunEvent runEvent =
        commonEventBuilder()
            .eventType(JobStatusUtil.fromJobStatus(event.newStatus()))
            .run(context.getOpenLineage().newRunBuilder().runId(context.getRunUuid()).build())
            .build();
    context.getEventEmitter().emit(runEvent);

    if (List.of(EventType.COMPLETE, EventType.START).contains(runEvent.getEventType())) {
      tracker.stopTracking();
    }
  }

  RunEventBuilder commonEventBuilder() {
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

  void loadJobId(JobCreatedEvent createdEvent) {
    String jobName =
        Optional.ofNullable(context.getConfig())
            .map(FlinkOpenLineageConfig::getJobConfig)
            .map(j -> j.getName())
            .orElse(createdEvent.jobName());

    String jobNamespace =
        Optional.ofNullable(context.getConfig())
            .map(FlinkOpenLineageConfig::getJobConfig)
            .map(j -> j.getName())
            .orElse(DEFAULT_NAMESPACE);

    JobIdentifier jobId =
        JobIdentifier.builder()
            .jobName(jobName)
            .jobNamespace(jobNamespace)
            .flinkJobId(createdEvent.jobId())
            .build();
    log.debug("JobIdentifier with jobId: {}", jobId.getFlinkJobId());
    context.setJobId(jobId);
  }
}
