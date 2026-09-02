/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle;

import io.openlineage.client.utils.UUIDUtils;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
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

/**
 * {@link ExecutionContext} for a Spark SQL execution whose start callback was processed after Spark
 * removed the execution's temporary `QueryExecution` mapping, which happens for fast queries or
 * when the Spark listener bus is backlogged. Instead of discarding the start callback, this context
 * buffers it together with a run id generated for the execution and the Spark execution id and root
 * execution id of the event.
 *
 * <p>The context is upgraded to a full {@link SparkSQLExecutionContext}, built with the buffered
 * run id, as soon as a query execution can be resolved - either by retrying the lookup on an
 * associated job callback, or from the mutable `qe` field of {@link SparkListenerSQLExecutionEnd}.
 * The upgrade happens once; the buffered start event is replayed before any other buffered or
 * current callback, preserving the original Spark event order and event time. Callbacks observed
 * before the upgrade (apart from the start event itself) are recorded as job ids only, so no Spark
 * scheduler or plan object is retained longer than necessary.
 *
 * <p>If neither the lookup nor the end event provides a query execution, a single warning is logged
 * and the buffered state is discarded, since the integration cannot produce a stable job identity
 * without the logical plan.
 */
@Slf4j
class PendingSparkSQLExecutionContext implements ExecutionContext {

  private final ContextFactory contextFactory;
  private final long executionId;
  private final long rootExecutionId;
  private final UUID runUuid = UUIDUtils.generateNewUUID();
  private final Set<Integer> jobIds = ConcurrentHashMap.newKeySet();

  /** The start callback buffered until the context is upgraded. */
  private SparkListenerSQLExecutionStart startEvent;

  /** The listener-level active job id buffered with the start callback. */
  private Integer activeJobId;

  /**
   * The most recent Spark job observed for this execution. It is replayed on upgrade when the
   * upgrade happens on a job callback, so the upgraded context can resolve datasets of the job
   * event that triggered the upgrade.
   */
  private ActiveJob activeJob;

  /** The context this pending context was upgraded to; set at most once. */
  private volatile ExecutionContext upgradedContext;

  PendingSparkSQLExecutionContext(
      ContextFactory contextFactory, long executionId, long rootExecutionId) {
    this.contextFactory = contextFactory;
    this.executionId = executionId;
    this.rootExecutionId = rootExecutionId;
  }

  /** Buffers the SQL start callback so it can be replayed once the context is upgraded. */
  @Override
  public void start(SparkListenerSQLExecutionStart startEvent) {
    if (upgraded()) {
      upgradedContext.start(startEvent);
      return;
    }
    if (this.startEvent == null) {
      this.startEvent = startEvent;
      log.debug(
          "Query execution of SQL execution {} is not available (yet); buffering start event",
          executionId);
    }
  }

  /**
   * Records the job id and retries the query execution lookup. If the lookup succeeds, the buffered
   * start event is emitted before the job start event.
   */
  @Override
  public void start(SparkListenerJobStart jobStart) {
    jobIds.add(jobStart.jobId());
    if (!upgradeFromExecutionId()) {
      log.debug(
          "Query execution of SQL execution {} is still not available; ignoring job start of job {}",
          executionId,
          jobStart.jobId());
      return;
    }
    upgradedContext.start(jobStart);
  }

  /** Records the job id and retries the query execution lookup like {@link #start}. */
  @Override
  public void end(SparkListenerJobEnd jobEnd) {
    jobIds.add(jobEnd.jobId());
    if (!upgradeFromExecutionId()) {
      log.debug(
          "Query execution of SQL execution {} is still not available; ignoring job end of job {}",
          executionId,
          jobEnd.jobId());
      return;
    }
    upgradedContext.end(jobEnd);
  }

  /**
   * Upgrades this context from the end event's query execution when no job callback succeeded
   * earlier, emits the buffered start event and finally the terminal event, both with the run id
   * buffered in this context. Logs a single warning and discards the buffered state when the end
   * event carries no query execution.
   */
  @Override
  public void end(SparkListenerSQLExecutionEnd endEvent) {
    ExecutionContext upgraded = upgradedContext;
    if (upgraded == null) {
      upgraded = orNull(contextFactory.createSparkSQLExecutionContext(endEvent, runUuid));
      if (upgraded == null) {
        log.warn(
            "OpenLineage could not resolve a query execution for Spark SQL execution {} "
                + "(root execution {}); no lineage events will be emitted for it, "
                + "observed job ids: {}",
            executionId,
            rootExecutionId,
            jobIds);
        discardBufferedState();
        return;
      }
      completeUpgrade(upgraded, false);
    }
    upgraded.end(endEvent);
  }

  @Override
  public void setActiveJobId(Integer activeJobId) {
    if (upgraded()) {
      upgradedContext.setActiveJobId(activeJobId);
      return;
    }
    this.activeJobId = activeJobId;
  }

  @Override
  public void setActiveJob(ActiveJob activeJob) {
    if (upgraded()) {
      upgradedContext.setActiveJob(activeJob);
      return;
    }
    this.activeJob = activeJob;
    if (activeJob != null) {
      jobIds.add(activeJob.jobId());
    }
  }

  @Override
  public Optional<Integer> getActiveJobId() {
    ExecutionContext upgraded = upgradedContext;
    return upgraded != null ? upgraded.getActiveJobId() : Optional.ofNullable(activeJobId);
  }

  @Override
  public void evictJob(int jobId) {
    if (upgraded()) {
      upgradedContext.evictJob(jobId);
    }
  }

  @Override
  public void clearRetainedState() {
    discardBufferedState();
    if (upgraded()) {
      upgradedContext.clearRetainedState();
    }
  }

  @Override
  public int getRetainedJobCount() {
    return upgraded() ? upgradedContext.getRetainedJobCount() : 0;
  }

  @Override
  public int getRetainedStageCount() {
    return upgraded() ? upgradedContext.getRetainedStageCount() : 0;
  }

  @Override
  public void start(SparkListenerApplicationStart applicationStart) {
    if (upgraded()) {
      upgradedContext.start(applicationStart);
    }
  }

  @Override
  public void end(SparkListenerApplicationEnd applicationEnd) {
    if (upgraded()) {
      upgradedContext.end(applicationEnd);
    }
  }

  @Override
  public void start(SparkListenerStageSubmitted stageSubmitted) {
    if (upgraded()) {
      upgradedContext.start(stageSubmitted);
    }
  }

  @Override
  public void end(SparkListenerStageCompleted stageCompleted) {
    if (upgraded()) {
      upgradedContext.end(stageCompleted);
    }
  }

  private boolean upgraded() {
    return upgradedContext != null;
  }

  /** Retries the query execution lookup for an associated job callback. */
  private boolean upgradeFromExecutionId() {
    if (upgraded()) {
      return true;
    }
    ExecutionContext upgraded =
        orNull(contextFactory.createSparkSQLExecutionContext(executionId, runUuid));
    if (upgraded == null) {
      return false;
    }
    completeUpgrade(upgraded, true);
    return true;
  }

  /**
   * Replays the buffered callbacks on the upgraded context in the order they were received, before
   * any other callback of the current execution is forwarded. The buffered start event keeps its
   * original Spark event time even though it is emitted late.
   */
  private void completeUpgrade(ExecutionContext upgraded, boolean replayActiveJob) {
    if (activeJobId != null) {
      upgraded.setActiveJobId(activeJobId);
    }
    if (startEvent != null) {
      upgraded.start(startEvent);
    }
    if (replayActiveJob && activeJob != null) {
      upgraded.setActiveJob(activeJob);
    }
    upgradedContext = upgraded;
    discardBufferedState();
    log.debug(
        "Upgraded pending SQL execution context of execution {} to {} with run id {}",
        executionId,
        upgraded.getClass().getSimpleName(),
        runUuid);
  }

  /**
   * Deliberately clears the buffered references so no Spark event or scheduler object is retained.
   */
  @SuppressWarnings("PMD.NullAssignment")
  private void discardBufferedState() {
    startEvent = null;
    activeJobId = null;
    activeJob = null;
  }

  /**
   * Tolerates a null value returned by a context factory, which Mockito mocks of {@code
   * ContextFactory} produce for unstubbed overloads.
   */
  private static ExecutionContext orNull(Optional<ExecutionContext> context) {
    return context == null ? null : context.orElse(null);
  }
}
