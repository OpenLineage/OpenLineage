/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.listener;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.RunEvent;
import io.openlineage.client.OpenLineage.RunEvent.EventType;
import io.openlineage.client.utils.UUIDUtils;
import io.openlineage.flink.client.EventEmitter;
import io.openlineage.flink.client.OpenLineageContext;
import io.openlineage.flink.client.OpenLineageContext.JobIdentifier;
import io.openlineage.flink.config.OpenlineageConfigParser;
import io.openlineage.flink.converter.LineageGraphConverter;
import io.openlineage.flink.util.JobStatusUtil;
import io.openlineage.flink.visitor.VisitorFactory;
import java.time.ZonedDateTime;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.core.execution.DefaultJobExecutionStatusEvent;
import org.apache.flink.core.execution.JobStatusChangedEvent;
import org.apache.flink.core.execution.JobStatusChangedListener;
import org.apache.flink.core.execution.JobStatusChangedListenerFactory.Context;
import org.apache.flink.streaming.runtime.execution.JobCreatedEvent;

@Slf4j
public class OpenLineageJobStatusChangedListener implements JobStatusChangedListener {
  public static final String DEFAULT_NAMESPACE = "flink-jobs";
  private final OpenLineageContext openLineageContext;
  private final EventEmitter eventEmitter;

  private final LineageGraphConverter graphConverter;

  public OpenLineageJobStatusChangedListener(Context context, VisitorFactory visitorFactory) {
    openLineageContext =
        OpenLineageContext.builder()
            .runId(UUIDUtils.generateNewUUID())
            .openLineage(new OpenLineage(EventEmitter.OPEN_LINEAGE_CLIENT_URI))
            .config(OpenlineageConfigParser.parse(context.getConfiguration()))
            .build();

    graphConverter = new LineageGraphConverter(openLineageContext, visitorFactory);
    eventEmitter = new EventEmitter(openLineageContext.getConfig());
  }

  @Override
  @SuppressWarnings("PMD.AvoidCatchingThrowable")
  public void onEvent(JobStatusChangedEvent event) {
    if (event instanceof JobCreatedEvent) {
      onJobCreatedEvent((JobCreatedEvent) event);
    } else if (event instanceof DefaultJobExecutionStatusEvent
        && openLineageContext.getJobId() != null) {
      onDefaultJobExecutionStatusEvent((DefaultJobExecutionStatusEvent) event);
    } else {
      log.warn("Unsupported event: {}", event.getClass());
    }
  }

  private void onJobCreatedEvent(JobCreatedEvent event) {
    loadJobId(event);
    log.debug("triggered onEvent for JobCreatedEvent: {}", event);
    try {
      eventEmitter.emit(graphConverter.convert(event.lineageGraph(), EventType.START));
    } catch (Exception e) {
      log.error("Triggering event caused an exception", e);
    }
  }

  private void onDefaultJobExecutionStatusEvent(DefaultJobExecutionStatusEvent event) {
    // only when job id has already been assigned
    RunEvent runEvent =
        openLineageContext
            .getOpenLineage()
            .newRunEventBuilder()
            .eventTime(ZonedDateTime.now())
            .eventType(
                JobStatusUtil.fromJobStatus(((DefaultJobExecutionStatusEvent) event).newStatus()))
            .run(
                openLineageContext
                    .getOpenLineage()
                    .newRunBuilder()
                    .runId(openLineageContext.getRunId())
                    .build())
            .job(
                openLineageContext
                    .getOpenLineage()
                    .newJobBuilder()
                    .namespace(openLineageContext.getJobId().getJobNamespace())
                    .name(openLineageContext.getJobId().getJobNme())
                    .build())
            .build();
    eventEmitter.emit(runEvent);
  }

  OpenLineageContext.JobIdentifier loadJobId(JobCreatedEvent createdEvent) {
    String jobName =
        Optional.ofNullable(openLineageContext.getConfig())
            .map(c -> c.getJobConfig())
            .map(j -> j.getName())
            .orElse(createdEvent.jobName());

    String jobNamespace =
        Optional.ofNullable(openLineageContext.getConfig())
            .map(c -> c.getJobConfig())
            .map(j -> j.getName())
            .orElse(DEFAULT_NAMESPACE);

    JobIdentifier jobId =
        JobIdentifier.builder().jobNme(jobName).jobNamespace(jobNamespace).build();

    openLineageContext.setJobId(jobId);
    return jobId;
  }
}
