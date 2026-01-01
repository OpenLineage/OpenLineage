/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle;

import static io.openlineage.client.OpenLineage.RunEvent.EventType.COMPLETE;
import static io.openlineage.client.OpenLineage.RunEvent.EventType.START;
import static io.openlineage.spark.agent.util.TimeUtils.toZonedTime;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.RunEvent;
import io.openlineage.spark.agent.EventEmitter;
import io.openlineage.spark.agent.filters.EventFilterUtils;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.naming.JobNameBuilder;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.scheduler.ActiveJob;
import org.apache.spark.scheduler.SparkListenerApplicationEnd;
import org.apache.spark.scheduler.SparkListenerApplicationStart;
import org.apache.spark.scheduler.SparkListenerJobEnd;
import org.apache.spark.scheduler.SparkListenerJobStart;
import org.apache.spark.scheduler.SparkListenerStageCompleted;
import org.apache.spark.scheduler.SparkListenerStageSubmitted;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart;

@Slf4j
class SparkApplicationExecutionContext implements ExecutionContext {
  private static final String SPARK_JOB_TYPE = "APPLICATION";
  private static final String SPARK_INTEGRATION = "SPARK";
  private static final String SPARK_PROCESSING_TYPE = "NONE";

  private final OpenLineageContext olContext;
  private final EventEmitter eventEmitter;
  private final OpenLineageRunEventBuilder runEventBuilder;

  public SparkApplicationExecutionContext(
      EventEmitter eventEmitter,
      OpenLineageContext olContext,
      OpenLineageRunEventBuilder runEventBuilder) {
    this.eventEmitter = eventEmitter;
    this.olContext = olContext;
    this.runEventBuilder = runEventBuilder;
  }

  @Override
  public void setActiveJob(ActiveJob activeJob) {}

  @Override
  public void start(SparkListenerJobStart jobStart) {}

  @Override
  public void start(SparkListenerSQLExecutionStart sqlStart) {}

  @Override
  public void start(SparkListenerStageSubmitted stageSubmitted) {}

  @Override
  public void end(SparkListenerJobEnd jobEnd) {}

  @Override
  public void end(SparkListenerSQLExecutionEnd sqlEnd) {}

  @Override
  public void end(SparkListenerStageCompleted stageCompleted) {}

  @Override
  public void start(SparkListenerApplicationStart applicationStart) {
    String applicationId =
        olContext.getSparkContext().map(context -> context.applicationId()).orElse(null);
    log.debug("SparkListenerApplicationStart - applicationId: {}", applicationId);
    if (EventFilterUtils.isDisabled(olContext, applicationStart)) {
      log.info(
          "OpenLineage received Spark event that is configured to be skipped: SparkListenerApplicationStart");
      return;
    }

    OpenLineage openLineage = olContext.getOpenLineage();
    RunEvent event =
        runEventBuilder.buildRun(
            OpenLineageRunEventContext.builder()
                .applicationParentRunFacet(buildApplicationParentFacet())
                .runEventBuilder(
                    openLineage
                        .newRunEventBuilder()
                        .eventTime(toZonedTime(applicationStart.time())))
                .eventType(START)
                .jobBuilder(getJobBuilder())
                .jobFacetsBuilder(getJobFacetsBuilder())
                .overwriteRunId(Optional.of(olContext.getApplicationUuid()))
                .event(applicationStart)
                .build());

    log.debug("Posting event for applicationId {} start: {}", applicationId, event);
    eventEmitter.emit(event);
  }

  @Override
  public void end(SparkListenerApplicationEnd applicationEnd) {
    String applicationId =
        olContext.getSparkContext().map(context -> context.applicationId()).orElse(null);
    log.debug("SparkListenerApplicationEnd - applicationId: {}", applicationId);
    if (EventFilterUtils.isDisabled(olContext, applicationEnd)) {
      log.info(
          "OpenLineage received Spark event that is configured to be skipped: SparkListenerApplicationEnd");
      return;
    }

    RunEvent event =
        runEventBuilder.buildRun(
            OpenLineageRunEventContext.builder()
                .applicationParentRunFacet(buildApplicationParentFacet())
                .runEventBuilder(
                    olContext
                        .getOpenLineage()
                        .newRunEventBuilder()
                        .eventTime(toZonedTime(applicationEnd.time())))
                .eventType(COMPLETE)
                .jobBuilder(getJobBuilder())
                .jobFacetsBuilder(getJobFacetsBuilder())
                .overwriteRunId(Optional.of(olContext.getApplicationUuid()))
                .event(applicationEnd)
                .build());

    log.debug("Posting event for applicationId {} end: {}", applicationId, event);
    eventEmitter.emit(event);
  }

  private OpenLineage.ParentRunFacet buildApplicationParentFacet() {
    if (eventEmitter.getParentRunId().isPresent()
        && eventEmitter.getParentJobName().isPresent()
        && eventEmitter.getParentJobNamespace().isPresent()) {
      OpenLineage ol = olContext.getOpenLineage();
      return ol.newParentRunFacet(
          ol.newParentRunFacetRun(eventEmitter.getParentRunId().get()),
          ol.newParentRunFacetJob(
              eventEmitter.getParentJobNamespace().get(), eventEmitter.getParentJobName().get()),
          ol.newParentRunFacetRoot(
              ol.newRootRun(
                  eventEmitter.getRootParentRunId().orElse(eventEmitter.getParentRunId().get())),
              ol.newRootJob(
                  eventEmitter
                      .getRootParentJobNamespace()
                      .orElse(eventEmitter.getParentJobNamespace().get()),
                  eventEmitter
                      .getRootParentJobName()
                      .orElse(eventEmitter.getParentJobName().get()))));
    }
    return null;
  }

  private OpenLineage.JobBuilder getJobBuilder() {
    return olContext
        .getOpenLineage()
        .newJobBuilder()
        .namespace(eventEmitter.getJobNamespace())
        .name(JobNameBuilder.build(olContext));
  }

  private OpenLineage.JobFacetsBuilder getJobFacetsBuilder() {
    return olContext
        .getOpenLineage()
        .newJobFacetsBuilder()
        .jobType(
            olContext
                .getOpenLineage()
                .newJobTypeJobFacetBuilder()
                .jobType(SPARK_JOB_TYPE)
                .processingType(SPARK_PROCESSING_TYPE)
                .integration(SPARK_INTEGRATION)
                .build());
  }
}
