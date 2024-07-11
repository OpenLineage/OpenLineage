/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle;

import static io.openlineage.client.OpenLineage.RunEvent.EventType.COMPLETE;
import static io.openlineage.client.OpenLineage.RunEvent.EventType.FAIL;
import static io.openlineage.client.OpenLineage.RunEvent.EventType.RUNNING;
import static io.openlineage.client.OpenLineage.RunEvent.EventType.START;
import static io.openlineage.spark.agent.util.TimeUtils.toZonedTime;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.RunEvent;
import io.openlineage.client.OpenLineage.RunEvent.EventType;
import io.openlineage.client.OpenLineageClientUtils;
import io.openlineage.spark.agent.EventEmitter;
import io.openlineage.spark.agent.Versions;
import io.openlineage.spark.agent.filters.EventFilterUtils;
import io.openlineage.spark.agent.util.PlanUtils;
import io.openlineage.spark.api.JobNameBuilder;
import io.openlineage.spark.api.OpenLineageContext;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.scheduler.ActiveJob;
import org.apache.spark.scheduler.JobFailed;
import org.apache.spark.scheduler.SparkListenerApplicationEnd;
import org.apache.spark.scheduler.SparkListenerApplicationStart;
import org.apache.spark.scheduler.SparkListenerJobEnd;
import org.apache.spark.scheduler.SparkListenerJobStart;
import org.apache.spark.scheduler.SparkListenerStageCompleted;
import org.apache.spark.scheduler.SparkListenerStageSubmitted;
import org.apache.spark.sql.execution.QueryExecution;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart;

@Slf4j
class SparkSQLExecutionContext implements ExecutionContext {

  private static final String NO_EXECUTION_INFO = "No execution info {}";
  private static final String SPARK_JOB_TYPE = "SQL_JOB";
  private static final String SPARK_INTEGRATION = "SPARK";
  private static final String SPARK_PROCESSING_TYPE_BATCH = "BATCH";
  private static final String SPARK_PROCESSING_TYPE_STREAMING = "STREAMING";
  private final long executionId;
  private String jobName;
  private final OpenLineageContext olContext;
  private final EventEmitter eventEmitter;
  private final OpenLineageRunEventBuilder runEventBuilder;
  private final OpenLineage openLineage = new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI);

  private boolean emittedOnSqlExecutionStart = false;
  private boolean emittedOnSqlExecutionEnd = false;
  private boolean emittedOnJobStart = false;
  private boolean emittedOnJobEnd = false;
  private Integer activeJobId;
  private AtomicBoolean finished = new AtomicBoolean(false);

  public SparkSQLExecutionContext(
      long executionId,
      EventEmitter eventEmitter,
      OpenLineageContext olContext,
      OpenLineageRunEventBuilder runEventBuilder) {
    this.executionId = executionId;
    this.eventEmitter = eventEmitter;
    this.olContext = olContext;
    this.runEventBuilder = runEventBuilder;
  }

  @Override
  public void start(SparkListenerSQLExecutionStart startEvent) {
    if (log.isDebugEnabled()) {
      log.debug("SparkListenerSQLExecutionStart - executionId: {}", startEvent.executionId());
    }
    if (!olContext.getQueryExecution().isPresent()) {
      log.info(NO_EXECUTION_INFO, olContext);
      return;
    } else if (EventFilterUtils.isDisabled(olContext, startEvent)) {
      log.info(
          "OpenLineage received Spark event that is configured to be skipped: SparkListenerSQLExecutionStart");
      // return;
    }

    olContext.setActiveJobId(activeJobId);
    // We shall skip this START event, focusing on the first SparkListenerJobStart event to be the START, because of the presence of the job nurn
    // only one START event is expected, in case it was already sent with jobStart, we send running
    // EventType eventType = emittedOnJobStart ? RUNNING : START;
    // emittedOnSqlExecutionStart = true;

    // RunEvent event =
    //     runEventBuilder.buildRun(
    //         OpenLineageRunEventContext.builder()
    //             .applicationParentRunFacet(buildApplicationParentFacet())
    //             .event(startEvent)
    //             .runEventBuilder(
    //                 openLineage
    //                     .newRunEventBuilder()
    //                     .eventTime(toZonedTime(startEvent.time()))
    //                     .eventType(eventType))
    //             .jobBuilder(buildJob())
    //             .jobFacetsBuilder(getJobFacetsBuilder(olContext.getQueryExecution().get()))
    //             .build());

    // log.debug("Posting event for start {}: {}", executionId, event);
    // eventEmitter.emit(event);
  }

  @Override
  public void end(SparkListenerSQLExecutionEnd endEvent) {
    if (log.isDebugEnabled()) {
      log.debug("SparkListenerSQLExecutionEnd - executionId: {}", endEvent.executionId());
    }
    // TODO: can we get failed event here?
    // If not, then we probably need to use this only for LogicalPlans that emit no Job events.
    // Maybe use QueryExecutionListener?
    olContext.setActiveJobId(activeJobId);
    if (!olContext.getQueryExecution().isPresent()) {
      log.info(NO_EXECUTION_INFO, olContext);
      return;
    } else if (EventFilterUtils.isDisabled(olContext, endEvent)) {
      log.info(
          "OpenLineage received Spark event that is configured to be skipped: SparkListenerSQLExecutionEnd");
      // return;
    }

    // only one COMPLETE event is expected, verify if jobEnd was not emitted
    EventType eventType;
    if (emittedOnJobStart && !emittedOnJobEnd) {
      // expecting jobEnd event later on
      eventType = RUNNING;
    } else {
      eventType = COMPLETE;
    }
    emittedOnSqlExecutionEnd = true;

    RunEvent event =
        runEventBuilder.buildRun(
            OpenLineageRunEventContext.builder()
                .applicationParentRunFacet(buildApplicationParentFacet())
                .event(endEvent)
                .runEventBuilder(
                    openLineage
                        .newRunEventBuilder()
                        .eventTime(toZonedTime(endEvent.time()))
                        .eventType(eventType))
                .jobBuilder(buildJob())
                .jobFacetsBuilder(getJobFacetsBuilder(olContext.getQueryExecution().get()))
                .build());

    if (log.isDebugEnabled()) {
      log.debug("Posting event for end {}: {}", executionId, OpenLineageClientUtils.toJson(event));
    }
    eventEmitter.emit(event);
  }

  // TODO: not invoked until https://github.com/OpenLineage/OpenLineage/issues/470 is completed
  @Override
  public void start(SparkListenerStageSubmitted stageSubmitted) {
    if (!olContext.getQueryExecution().isPresent()) {
      log.info(NO_EXECUTION_INFO, olContext);
      return;
    } else if (EventFilterUtils.isDisabled(olContext, stageSubmitted)) {
      log.info(
          "OpenLineage received Spark event that is configured to be skipped: SparkListenerStageSubmitted");
      return;
    }

    RunEvent event =
        runEventBuilder.buildRun(
            OpenLineageRunEventContext.builder()
                .applicationParentRunFacet(buildApplicationParentFacet())
                .event(stageSubmitted)
                .runEventBuilder(
                    openLineage
                        .newRunEventBuilder()
                        .eventTime(ZonedDateTime.now(ZoneOffset.UTC))
                        .eventType(RUNNING))
                .jobBuilder(buildJob())
                .jobFacetsBuilder(getJobFacetsBuilder(olContext.getQueryExecution().get()))
                .build());

    log.debug("Posting event for stage submitted {}: {}", executionId, event);
    eventEmitter.emit(event);
  }

  // TODO: not invoked until https://github.com/OpenLineage/OpenLineage/issues/470 is completed
  @Override
  public void end(SparkListenerStageCompleted stageCompleted) {
    if (!olContext.getQueryExecution().isPresent()) {
      log.info(NO_EXECUTION_INFO, olContext);
      return;
    } else if (EventFilterUtils.isDisabled(olContext, stageCompleted)) {
      log.info(
          "OpenLineage received Spark event that is configured to be skipped: SparkListenerStageCompleted");
      return;
    }
    RunEvent event =
        runEventBuilder.buildRun(
            OpenLineageRunEventContext.builder()
                .applicationParentRunFacet(buildApplicationParentFacet())
                .event(stageCompleted)
                .runEventBuilder(
                    openLineage
                        .newRunEventBuilder()
                        .eventTime(ZonedDateTime.now(ZoneOffset.UTC))
                        .eventType(RUNNING))
                .jobBuilder(buildJob())
                .jobFacetsBuilder(getJobFacetsBuilder(olContext.getQueryExecution().get()))
                .build());

    log.debug("Posting event for stage completed {}: {}", executionId, event);
    eventEmitter.emit(event);
  }

  @Override
  public Optional<Integer> getActiveJobId() {
    return Optional.ofNullable(activeJobId);
  }

  @Override
  public void setActiveJobId(Integer activeJobId) {
    this.activeJobId = activeJobId;
  }

  @Override
  public void setActiveJob(ActiveJob activeJob) {
    olContext.setActiveJobId(activeJob.jobId());
    runEventBuilder.registerJob(activeJob);
    log.debug("Registering jobId: {} into runUid: {}", activeJob, olContext.getRunUuid());
  }

  @Override
  public void start(SparkListenerJobStart jobStart) {
    log.debug("SparkListenerJobStart - executionId: {}", executionId);
    try {
      jobName = jobStart.properties().getProperty("spark.job.name");
    } catch (RuntimeException e) {
      log.info("spark.job.name property not found in the context");
    }
    olContext.setJobNurn(jobName);
    if (!olContext.getQueryExecution().isPresent()) {
      log.info(NO_EXECUTION_INFO, olContext);
      return;
    } else if (EventFilterUtils.isDisabled(olContext, jobStart)) {
      log.info(
          "OpenLineage received Spark event that is configured to be skipped: SparkListenerJobStart");
      // return;
    }

    // only one START event is expected, in case it was already sent with sqlExecutionStart, we send
    // running
    EventType eventType = emittedOnSqlExecutionStart ? RUNNING : START;
    emittedOnSqlExecutionStart = true;
    emittedOnJobStart = true;

    RunEvent event =
        runEventBuilder.buildRun(
            OpenLineageRunEventContext.builder()
                .applicationParentRunFacet(buildApplicationParentFacet())
                .event(jobStart)
                .runEventBuilder(
                    openLineage
                        .newRunEventBuilder()
                        .eventTime(toZonedTime(jobStart.time()))
                        .eventType(eventType))
                .jobBuilder(buildJob())
                .jobFacetsBuilder(getJobFacetsBuilder(olContext.getQueryExecution().get()))
                .build());

    log.debug("Posting event for start {}: {}", executionId, event);
    eventEmitter.emit(event);
  }

  @Override
  public void end(SparkListenerJobEnd jobEnd) {
    log.debug("SparkListenerJobEnd - executionId: {}", executionId);
    olContext.setActiveJobId(jobEnd.jobId());
    if (!finished.compareAndSet(false, true)) {
      log.debug("Event already finished, returning");
      return;
    }

    if (!olContext.getQueryExecution().isPresent()) {
      log.info(NO_EXECUTION_INFO, olContext);
      return;
    } else if (EventFilterUtils.isDisabled(olContext, jobEnd)) {
      log.info(
          "OpenLineage received Spark event that is configured to be skipped: SparkListenerJobEnd");
      // return;
    }

    // only one COMPLETE event is expected,
    EventType eventType;
    if (jobEnd.jobResult() instanceof JobFailed) {
      eventType = FAIL;
    } else if (emittedOnSqlExecutionStart && !emittedOnSqlExecutionEnd) {
      // still waiting for sqlExecutionEnd event which will emit COMPLETE event
      eventType = RUNNING;
    } else {
      eventType = COMPLETE;
    }
    emittedOnJobEnd = true;

    RunEvent event =
        runEventBuilder.buildRun(
            OpenLineageRunEventContext.builder()
                .applicationParentRunFacet(buildApplicationParentFacet())
                .event(jobEnd)
                .runEventBuilder(
                    openLineage
                        .newRunEventBuilder()
                        .eventTime(toZonedTime(jobEnd.time()))
                        .eventType(eventType))
                .jobBuilder(buildJob())
                .jobFacetsBuilder(getJobFacetsBuilder(olContext.getQueryExecution().get()))
                .build());

    log.debug("Posting event for end {}: {}", executionId, event);
    eventEmitter.emit(event);
  }

  @Override
  public void start(SparkListenerApplicationStart applicationStart) {}

  @Override
  public void end(SparkListenerApplicationEnd applicationEnd) {}

  private OpenLineage.ParentRunFacet buildApplicationParentFacet() {
    return PlanUtils.parentRunFacet(
        eventEmitter.getApplicationRunId(),
        eventEmitter.getApplicationJobName(),
        eventEmitter.getJobNamespace());
  }

  protected OpenLineage.JobBuilder buildJob() {
    return openLineage
        .newJobBuilder()
        .name(JobNameBuilder.build(olContext))
        .namespace(this.eventEmitter.getJobNamespace());
  }

  /**
   * Getting the job type facet for Spark jobs. Values: job type: `SQL_JOB`, job integration:
   * `SPARK`, processing type: can be `batch` or `streaming` based on
   * queryExecution.optimizedPlan().isStreaming()
   *
   * @param queryExecution
   * @return OpenLineage.JobTypeJobFacet
   */
  private OpenLineage.JobTypeJobFacet getJobTypeJobFacet(QueryExecution queryExecution) {
    final String processingType;
    // Determine processing type
    if (queryExecution.optimizedPlan().isStreaming()) {
      processingType = SPARK_PROCESSING_TYPE_STREAMING;
    } else {
      processingType = SPARK_PROCESSING_TYPE_BATCH;
    }

    return openLineage
        .newJobTypeJobFacetBuilder()
        .jobType(SPARK_JOB_TYPE)
        .processingType(processingType)
        .integration(SPARK_INTEGRATION)
        .build();
  }

  private OpenLineage.JobFacetsBuilder getJobFacetsBuilder(QueryExecution queryExecution) {
    return openLineage.newJobFacetsBuilder().jobType(getJobTypeJobFacet(queryExecution));
  }
}
