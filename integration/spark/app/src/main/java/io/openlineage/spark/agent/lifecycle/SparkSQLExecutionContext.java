/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.RunEvent;
import io.openlineage.client.OpenLineageClientUtils;
import io.openlineage.spark.agent.EventEmitter;
import io.openlineage.spark.agent.Versions;
import io.openlineage.spark.agent.filters.EventFilterUtils;
import io.openlineage.spark.agent.util.PlanUtils;
import io.openlineage.spark.api.OpenLineageContext;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Locale;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkContext;
import org.apache.spark.scheduler.ActiveJob;
import org.apache.spark.scheduler.SparkListenerJobEnd;
import org.apache.spark.scheduler.SparkListenerJobStart;
import org.apache.spark.scheduler.SparkListenerStageCompleted;
import org.apache.spark.scheduler.SparkListenerStageSubmitted;
import org.apache.spark.sql.execution.QueryExecution;
import org.apache.spark.sql.execution.SparkPlan;
import org.apache.spark.sql.execution.WholeStageCodegenExec;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart;

@Slf4j
class SparkSQLExecutionContext implements ExecutionContext {

  private static final String NO_EXECUTION_INFO = "No execution info {}";
  private final long executionId;
  private final OpenLineageContext olContext;
  private final EventEmitter eventEmitter;
  private final OpenLineageRunEventBuilder runEventBuilder;
  private final OpenLineage openLineage = new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI);

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
    log.debug("SparkListenerSQLExecutionStart - executionId: {}", startEvent.executionId());
    if (!olContext.getQueryExecution().isPresent()) {
      log.info(NO_EXECUTION_INFO, olContext);
      return;
    } else if (EventFilterUtils.isDisabled(olContext, startEvent)) {
      log.info(
          "OpenLineage received Spark event that is configured to be skipped: SparkListenerSQLExecutionStart");
      return;
    }
    RunEvent event =
        runEventBuilder.buildRun(
            buildParentFacet(),
            openLineage.newRunEventBuilder().eventTime(toZonedTime(startEvent.time())),
            buildJob(olContext.getQueryExecution().get()),
            startEvent);

    log.debug("Posting event for start {}: {}", executionId, event);
    eventEmitter.emit(event);
  }

  @Override
  public void end(SparkListenerSQLExecutionEnd endEvent) {
    log.debug("SparkListenerSQLExecutionEnd - executionId: {}", endEvent.executionId());
    // TODO: can we get failed event here?
    // If not, then we probably need to use this only for LogicalPlans that emit no Job events.
    // Maybe use QueryExecutionListener?
    if (!olContext.getQueryExecution().isPresent()) {
      log.info(NO_EXECUTION_INFO, olContext);
      return;
    } else if (EventFilterUtils.isDisabled(olContext, endEvent)) {
      log.info(
          "OpenLineage received Spark event that is configured to be skipped: SparkListenerSQLExecutionEnd");
      return;
    }
    RunEvent event =
        runEventBuilder.buildRun(
            buildParentFacet(),
            openLineage.newRunEventBuilder().eventTime(toZonedTime(endEvent.time())),
            buildJob(olContext.getQueryExecution().get()),
            endEvent);

    log.debug("Posting event for end {}: {}", executionId, OpenLineageClientUtils.toJson(event));
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
            buildParentFacet(),
            openLineage.newRunEventBuilder().eventTime(ZonedDateTime.now(ZoneOffset.UTC)),
            buildJob(olContext.getQueryExecution().get()),
            stageSubmitted);

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
            buildParentFacet(),
            openLineage.newRunEventBuilder().eventTime(ZonedDateTime.now(ZoneOffset.UTC)),
            buildJob(olContext.getQueryExecution().get()),
            stageCompleted);

    log.debug("Posting event for stage completed {}: {}", executionId, event);

    eventEmitter.emit(event);
  }

  @Override
  public void setActiveJob(ActiveJob activeJob) {
    runEventBuilder.registerJob(activeJob);
  }

  @Override
  public void start(SparkListenerJobStart jobStart) {
    log.debug("SparkListenerJobStart - executionId: " + executionId);
    if (!olContext.getQueryExecution().isPresent()) {
      log.info(NO_EXECUTION_INFO, olContext);
      return;
    } else if (EventFilterUtils.isDisabled(olContext, jobStart)) {
      log.info(
          "OpenLineage received Spark event that is configured to be skipped: SparkListenerJobStart");
      return;
    }
    RunEvent event =
        runEventBuilder.buildRun(
            buildParentFacet(),
            openLineage.newRunEventBuilder().eventTime(toZonedTime(jobStart.time())),
            buildJob(olContext.getQueryExecution().get()),
            jobStart);

    log.debug("Posting event for start {}: {}", executionId, event);
    eventEmitter.emit(event);
  }

  @Override
  public void end(SparkListenerJobEnd jobEnd) {
    log.debug("SparkListenerJobEnd - executionId: " + executionId);
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
      return;
    }
    RunEvent event =
        runEventBuilder.buildRun(
            buildParentFacet(),
            openLineage.newRunEventBuilder().eventTime(toZonedTime(jobEnd.time())),
            buildJob(olContext.getQueryExecution().get()),
            jobEnd);

    log.debug("Posting event for end {}: {}", executionId, event);
    eventEmitter.emit(event);
  }

  private Optional<OpenLineage.ParentRunFacet> buildParentFacet() {
    return eventEmitter
        .getParentRunId()
        .map(
            runId ->
                PlanUtils.parentRunFacet(
                    runId, eventEmitter.getParentJobName(), eventEmitter.getJobNamespace()));
  }

  protected ZonedDateTime toZonedTime(long time) {
    Instant i = Instant.ofEpochMilli(time);
    return ZonedDateTime.ofInstant(i, ZoneOffset.UTC);
  }

  protected OpenLineage.JobBuilder buildJob(QueryExecution queryExecution) {
    SparkContext sparkContext = queryExecution.executedPlan().sparkContext();
    SparkPlan node = queryExecution.executedPlan();

    // Unwrap SparkPlan from WholeStageCodegen, as that's not a descriptive or helpful job name
    if (node instanceof WholeStageCodegenExec) {
      node = ((WholeStageCodegenExec) node).child();
    }

    String name = eventEmitter.getAppName().orElse(sparkContext.appName());
    return openLineage
        .newJobBuilder()
        .namespace(this.eventEmitter.getJobNamespace())
        .name(normalizeName(name) + "." + normalizeName(node.nodeName()));
  }

  // normalizes string, changes CamelCase to snake_case and replaces all non-alphanumerics with '_'
  private static String normalizeName(String name) {
    return name.replaceAll(CAMEL_TO_SNAKE_CASE, "_$1").toLowerCase(Locale.ROOT);
  }
}
