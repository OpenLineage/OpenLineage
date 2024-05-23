/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle;

import static io.openlineage.client.OpenLineage.RunEvent.EventType.COMPLETE;
import static io.openlineage.client.OpenLineage.RunEvent.EventType.START;
import static io.openlineage.spark.agent.util.TimeUtils.toZonedTime;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.RunEvent;
import io.openlineage.spark.agent.EventEmitter;
import io.openlineage.spark.agent.Versions;
import io.openlineage.spark.agent.filters.EventFilterUtils;
import io.openlineage.spark.api.OpenLineageContext;
import java.util.Locale;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkContext;
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
  private final OpenLineageContext olContext;
  private final EventEmitter eventEmitter;
  private final OpenLineageRunEventBuilder runEventBuilder;
  private final OpenLineage openLineage = new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI);

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

    RunEvent event =
        runEventBuilder.buildRun(
            buildApplicationParentFacet(),
            openLineage
                .newRunEventBuilder()
                .eventTime(toZonedTime(applicationStart.time()))
                .eventType(START),
            buildJob(),
            openLineage.newJobFacetsBuilder(),
            applicationStart);

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
            buildApplicationParentFacet(),
            openLineage
                .newRunEventBuilder()
                .eventTime(toZonedTime(applicationEnd.time()))
                .eventType(COMPLETE),
            buildJob(),
            openLineage.newJobFacetsBuilder(),
            applicationEnd);

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
              eventEmitter.getParentJobNamespace().get(), eventEmitter.getParentJobName().get()));
    }
    return null;
  }

  protected OpenLineage.JobBuilder buildJob() {
    String name =
        eventEmitter
            .getOverriddenAppName()
            .orElse(olContext.getSparkContext().map(SparkContext::appName).orElse("unknown"));
    return openLineage
        .newJobBuilder()
        .namespace(eventEmitter.getJobNamespace())
        .name(normalizeName(name));
  }

  // normalizes string, changes CamelCase to snake_case and replaces all non-alphanumerics with '_'
  private static String normalizeName(String name) {
    return name.replaceAll(CAMEL_TO_SNAKE_CASE, "_$1").toLowerCase(Locale.ROOT);
  }
}
