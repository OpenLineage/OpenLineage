/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.listener;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.ColumnLineageDatasetFacet;
import io.openlineage.client.OpenLineage.RunEvent;
import io.openlineage.client.OpenLineage.RunEvent.EventType;
import io.openlineage.client.utils.UUIDUtils;
import io.openlineage.flink.client.EventEmitter;
import io.openlineage.flink.client.OpenLineageContext;
import io.openlineage.flink.client.OpenLineageContext.JobIdentifier;
import io.openlineage.flink.column.QueryOperationConverter;
import io.openlineage.flink.config.OpenlineageConfigParser;
import io.openlineage.flink.converter.LineageGraphConverter;
import io.openlineage.flink.util.DatasetUtil;
import io.openlineage.flink.util.JobStatusUtil;
import io.openlineage.flink.visitor.VisitorFactory;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.JobID;
import org.apache.flink.core.execution.DefaultJobExecutionStatusEvent;
import org.apache.flink.core.execution.JobStatusChangedEvent;
import org.apache.flink.core.execution.JobStatusChangedListener;
import org.apache.flink.core.execution.JobStatusChangedListenerFactory.Context;
import org.apache.flink.streaming.runtime.execution.JobCreatedEvent;
import org.apache.flink.table.runtime.execution.QueryOperationEvent;

@Slf4j
public class OpenLineageJobStatusChangedListener implements JobStatusChangedListener {
  public static final String DEFAULT_NAMESPACE = "flink-jobs";
  private final OpenLineageContext openLineageContext;
  private final EventEmitter eventEmitter;

  private final LineageGraphConverter graphConverter;
  private final QueryOperationConverter queryOperationConverter;

  /**
   * Stores start RunEvents. This is used to emit separate events with column level lineage. Those
   * events are necessary as information used to collect column level lineage is shared with
   * QueryOperationEvent that is emitted after the JobCreatedEvent.
   */
  private static final Map<JobID, RunEvent> startEvents = new HashMap<>();

  public OpenLineageJobStatusChangedListener(Context context, VisitorFactory visitorFactory) {
    openLineageContext =
        OpenLineageContext.builder()
            .runId(UUIDUtils.generateNewUUID())
            .openLineage(new OpenLineage(EventEmitter.OPEN_LINEAGE_CLIENT_URI))
            .config(OpenlineageConfigParser.parse(context.getConfiguration()))
            .build();

    graphConverter = new LineageGraphConverter(openLineageContext, visitorFactory);
    eventEmitter = new EventEmitter(openLineageContext.getConfig());
    queryOperationConverter = new QueryOperationConverter(openLineageContext);
  }

  @Override
  @SuppressWarnings("PMD.AvoidCatchingThrowable")
  public void onEvent(JobStatusChangedEvent event) {
    if (event instanceof JobCreatedEvent) {
      onJobCreatedEvent((JobCreatedEvent) event);
    } else if (event instanceof DefaultJobExecutionStatusEvent
        && openLineageContext.getJobId() != null) {
      onDefaultJobExecutionStatusEvent((DefaultJobExecutionStatusEvent) event);
    } else if (event instanceof QueryOperationEvent) {
      onQueryOperationEvent((QueryOperationEvent) event);
    } else {
      log.warn("Unsupported event: {}", event.getClass());
    }
  }

  private void onJobCreatedEvent(JobCreatedEvent event) {
    log.debug("triggered onEvent for JobCreatedEvent: {}", event);
    loadJobId(event);
    try {
      RunEvent runEvent = graphConverter.convert(event.lineageGraph(), EventType.START);
      startEvents.put(event.jobId(), runEvent);
      eventEmitter.emit(runEvent);
    } catch (Exception e) {
      log.error("Triggering event caused an exception", e);
    }
  }

  private void onQueryOperationEvent(QueryOperationEvent event) {
    log.debug("triggered onQueryOperationEvent for QueryOperationEvent: {}", event);
    try {
      RunEvent startEvent = startEvents.get(event.jobId());
      if (startEvent == null) {
        log.error("No start event found for job id: {}", event.jobId());
        return;
      } else if (startEvent.getInputs().isEmpty()) {
        log.error("No inputs found for job id: {}", event.jobId());
        return;
      } else if (startEvent.getOutputs().isEmpty()) {
        log.error("No outputs found for job id: {}", event.jobId());
        return;
      } else if (startEvent.getOutputs().get(0).getFacets() == null) {
        log.error("No facets found for startEvent within job id: {}", event.jobId());
        return;
      } else if (startEvent.getOutputs().get(0).getFacets().getSchema() == null) {
        log.error("No schema facet found for startEvent within job id: {}", event.jobId());
        return;
      }

      RunEvent runEvent =
          openLineageContext
              .getOpenLineage()
              .newRunEventBuilder()
              .eventTime(ZonedDateTime.now())
              .eventType(EventType.RUNNING)
              .run(startEvent.getRun())
              .inputs(startEvent.getInputs())
              .outputs(
                  startEvent.getOutputs().stream()
                      .map(
                          dataset -> {
                            Optional<ColumnLineageDatasetFacet> columnLineageDatasetFacet =
                                queryOperationConverter.convert(
                                    startEvent.getInputs(),
                                    dataset.getFacets().getSchema(),
                                    event.queryOperation());
                            return columnLineageDatasetFacet
                                .map(
                                    lineageDatasetFacet ->
                                        DatasetUtil.enrichDataset(
                                            openLineageContext.getOpenLineage(),
                                            dataset,
                                            lineageDatasetFacet))
                                .orElse(dataset);
                          })
                      .collect(Collectors.toList()))
              .job(startEvent.getJob())
              .build();
      eventEmitter.emit(runEvent);
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
