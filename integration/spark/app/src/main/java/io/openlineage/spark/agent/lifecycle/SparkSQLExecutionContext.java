/*
/* Copyright 2018-2026 contributors to the OpenLineage project
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
import io.openlineage.spark.agent.filters.EventFilterUtils;
import io.openlineage.spark.agent.util.PlanUtils;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.naming.JobNameBuilder;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.Stack;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.scheduler.ActiveJob;
import org.apache.spark.scheduler.JobFailed;
import org.apache.spark.scheduler.SparkListenerApplicationEnd;
import org.apache.spark.scheduler.SparkListenerApplicationStart;
import org.apache.spark.scheduler.SparkListenerJobEnd;
import org.apache.spark.scheduler.SparkListenerJobStart;
import org.apache.spark.scheduler.SparkListenerStageCompleted;
import org.apache.spark.scheduler.SparkListenerStageSubmitted;
import org.apache.spark.scheduler.StageInfo;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
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
  private final OpenLineageContext olContext;
  private final EventEmitter eventEmitter;
  private final OpenLineageRunEventBuilder runEventBuilder;

  private static final String CHECKPOINT_METHOD = "checkpoint";
  private static final String LOCAL_CHECKPOINT_METHOD = "localCheckpoint";

  private boolean emittedOnSqlExecutionStart = false;
  private boolean emittedOnSqlExecutionEnd = false;
  private boolean emittedOnJobStart = false;
  private boolean emittedOnJobEnd = false;
  private Integer activeJobId;
  private AtomicBoolean finished = new AtomicBoolean(false);

  /**
   * Set when this execution corresponds to Spark materializing a {@code checkpoint()}/{@code
   * localCheckpoint()} call rather than a user query. Such executions run under their own {@code
   * SQLExecution} (wrapping the plan of the dataset being checkpointed), so they must not produce
   * OpenLineage events of their own - instead, their lineage is captured and stashed in {@link
   * io.openlineage.spark.api.CheckpointContext} to be reattached once the checkpointed RDD is read
   * downstream.
   */
  private boolean isCheckpointExecution = false;

  /**
   * Long form of the call site of the {@code checkpoint()}/{@code localCheckpoint()} call, as
   * reported by {@link SparkListenerSQLExecutionStart#details()} - the same as the long form of the
   * checkpointed RDD's creation site.
   */
  private String checkpointCallSite;

  private boolean isLocalCheckpoint = false;

  /**
   * Highest id of an RDD computed by a job run within a non-local checkpoint execution. The
   * checkpointed RDD is created after all the RDDs of the plan being checkpointed, so for an eager
   * checkpoint it's the one with the highest id. A lazy checkpoint runs no job computing it - see
   * {@link #resolveCheckpointRddId()}.
   */
  private Optional<Integer> nonLocalCheckpointRddId = Optional.empty();

  private SparkSQLQueryParser sqlRecorder = new SparkSQLQueryParser();

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

    Optional<String> checkpointMethod =
        isCheckpointLineageEnabled()
            ? checkpointMethod(startEvent.details())
            : Optional.<String>empty();
    if (checkpointMethod.isPresent()) {
      isCheckpointExecution = true;
      isLocalCheckpoint = LOCAL_CHECKPOINT_METHOD.equals(checkpointMethod.get());
      checkpointCallSite = startEvent.details();
      log.debug(
          "Not emitting OpenLineage events for checkpoint execution: {}", startEvent.description());
      return;
    }
    if (!olContext.getQueryExecution().isPresent()) {
      log.info(NO_EXECUTION_INFO, olContext);
      return;
    } else if (EventFilterUtils.isDisabled(olContext, startEvent)) {
      log.info(
          "OpenLineage received Spark event that is configured to be skipped: SparkListenerSQLExecutionStart");
      return;
    }

    olContext.setActiveJobId(activeJobId);
    // only one START event is expected, in case it was already sent with jobStart, we send running
    EventType eventType = emittedOnJobStart ? RUNNING : START;
    emittedOnSqlExecutionStart = true;

    RunEvent event =
        runEventBuilder.buildRun(
            OpenLineageRunEventContext.builder()
                .applicationParentRunFacet(buildApplicationParentFacet())
                .event(startEvent)
                .runEventBuilder(
                    olContext
                        .getOpenLineage()
                        .newRunEventBuilder()
                        .eventTime(toZonedTime(startEvent.time())))
                .eventType(eventType)
                .jobBuilder(buildJob())
                .jobFacetsBuilder(getJobFacetsBuilder(olContext.getQueryExecution().get()))
                .build());

    if (log.isDebugEnabled()) {
      log.debug(
          "Posting event for start {}: {}", executionId, OpenLineageClientUtils.toJson(event));
    }

    eventEmitter.emit(event);
  }

  @Override
  public void end(SparkListenerSQLExecutionEnd endEvent) {
    if (log.isDebugEnabled()) {
      log.debug("SparkListenerSQLExecutionEnd - executionId: {}", endEvent.executionId());
    }
    if (isCheckpointExecution) {
      captureCheckpointLineage(endEvent);
      return;
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
      return;
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
                    olContext
                        .getOpenLineage()
                        .newRunEventBuilder()
                        .eventTime(toZonedTime(endEvent.time())))
                .eventType(eventType)
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
                    olContext
                        .getOpenLineage()
                        .newRunEventBuilder()
                        .eventTime(ZonedDateTime.now(ZoneOffset.UTC)))
                .eventType(RUNNING)
                .jobBuilder(buildJob())
                .jobFacetsBuilder(getJobFacetsBuilder(olContext.getQueryExecution().get()))
                .build());

    if (log.isDebugEnabled()) {
      log.debug(
          "Posting event for stage submitted {}: {}",
          executionId,
          OpenLineageClientUtils.toJson(event));
    }
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
                    olContext
                        .getOpenLineage()
                        .newRunEventBuilder()
                        .eventTime(ZonedDateTime.now(ZoneOffset.UTC)))
                .eventType(RUNNING)
                .jobBuilder(buildJob())
                .jobFacetsBuilder(getJobFacetsBuilder(olContext.getQueryExecution().get()))
                .build());

    if (log.isDebugEnabled()) {
      log.debug(
          "Posting event for stage completed {}: {}",
          executionId,
          OpenLineageClientUtils.toJson(event));
    }
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
  public void setCommandChildExecution(boolean commandChildExecution) {
    olContext.setCommandChildExecution(Optional.of(commandChildExecution));
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
    olContext.setActiveJobId(jobStart.jobId());
    if (isCheckpointExecution) {
      recordCheckpointJob(jobStart);
      return;
    }
    if (!olContext.getQueryExecution().isPresent()) {
      log.info(NO_EXECUTION_INFO, olContext);
      return;
    } else if (EventFilterUtils.isDisabled(olContext, jobStart)) {
      log.info(
          "OpenLineage received Spark event that is configured to be skipped: SparkListenerJobStart");
      return;
    }

    // only one START event is expected, in case it was already sent with sqlExecutionStart, we send
    // running
    EventType eventType = emittedOnSqlExecutionStart ? RUNNING : START;
    emittedOnJobStart = true;

    RunEvent event =
        runEventBuilder.buildRun(
            OpenLineageRunEventContext.builder()
                .applicationParentRunFacet(buildApplicationParentFacet())
                .event(jobStart)
                .runEventBuilder(
                    olContext
                        .getOpenLineage()
                        .newRunEventBuilder()
                        .eventTime(toZonedTime(jobStart.time())))
                .eventType(eventType)
                .jobBuilder(buildJob())
                .jobFacetsBuilder(getJobFacetsBuilder(olContext.getQueryExecution().get()))
                .build());

    if (log.isDebugEnabled()) {
      log.debug(
          "Posting event for start {}: {}", executionId, OpenLineageClientUtils.toJson(event));
    }

    eventEmitter.emit(event);
  }

  @Override
  public void end(SparkListenerJobEnd jobEnd) {
    try {
      log.debug("SparkListenerJobEnd - executionId: {}", executionId);
      olContext.setActiveJobId(jobEnd.jobId());
      if (isCheckpointExecution) {
        return;
      }
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
                      olContext
                          .getOpenLineage()
                          .newRunEventBuilder()
                          .eventTime(toZonedTime(jobEnd.time())))
                  .eventType(eventType)
                  .jobBuilder(buildJob())
                  .jobFacetsBuilder(getJobFacetsBuilder(olContext.getQueryExecution().get()))
                  .build());

      if (log.isDebugEnabled()) {
        log.debug(
            "Posting event for end {}: {}", executionId, OpenLineageClientUtils.toJson(event));
      }

      eventEmitter.emit(event);
    } finally {
      runEventBuilder.evictJob(jobEnd.jobId());
    }
  }

  @Override
  public void evictJob(int jobId) {
    runEventBuilder.evictJob(jobId);
  }

  @Override
  public void clearRetainedState() {
    runEventBuilder.clearRetainedState();
  }

  @Override
  public int getRetainedJobCount() {
    return runEventBuilder.getRetainedJobCount();
  }

  @Override
  public int getRetainedStageCount() {
    return runEventBuilder.getRetainedStageCount();
  }

  @Override
  public void start(SparkListenerApplicationStart applicationStart) {}

  @Override
  public void end(SparkListenerApplicationEnd applicationEnd) {}

  /**
   * Returns {@code checkpoint} or {@code localCheckpoint} if the execution was started by the
   * respective {@code Dataset} method, based on its call site.
   *
   * <p>The first line of a call site's long form is the outermost Spark frame - the Spark API
   * method called by the user (see {@code org.apache.spark.util.Utils#getCallSite}). Unlike the
   * execution's description, it doesn't change when a job description is set (e.g. with {@code
   * SparkContext#setJobDescription}), and it can't be matched by a description or another library's
   * {@code checkpoint} method. It's only replaced when a call site is set by hand with {@code
   * SparkContext#setCallSite}, in which case the checkpoint isn't detected and emits regular events
   * instead.
   */
  static Optional<String> checkpointMethod(String callSite) {
    if (callSite == null) {
      return Optional.empty();
    }
    int lineEnd = callSite.indexOf('\n');
    String outermostSparkFrame = lineEnd < 0 ? callSite : callSite.substring(0, lineEnd);
    int argsStart = outermostSparkFrame.indexOf('(');
    if (argsStart < 0) {
      return Optional.empty();
    }
    String qualifiedMethod = outermostSparkFrame.substring(0, argsStart);
    int methodStart = qualifiedMethod.lastIndexOf('.');
    if (methodStart < 0) {
      return Optional.empty();
    }
    String className = qualifiedMethod.substring(0, methodStart);
    String method = qualifiedMethod.substring(methodStart + 1);
    // org.apache.spark.sql.Dataset up to Spark 3.5, org.apache.spark.sql.classic.Dataset in 4.0+
    boolean isDataset =
        className.startsWith("org.apache.spark.sql.") && className.endsWith(".Dataset");
    if (isDataset && (CHECKPOINT_METHOD.equals(method) || LOCAL_CHECKPOINT_METHOD.equals(method))) {
      return Optional.of(method);
    }
    return Optional.empty();
  }

  /**
   * Whether capturing lineage across {@code checkpoint()}/{@code localCheckpoint()} boundaries is
   * enabled. Disabled by default - see {@link
   * io.openlineage.spark.api.SparkOpenLineageConfig.CheckpointConfig}.
   */
  private boolean isCheckpointLineageEnabled() {
    return olContext.getOpenLineageConfig().getCheckpointConfig().getEnabled();
  }

  /**
   * Records the RDD computed by a job run within a non-local checkpoint execution - that's the RDD
   * of the job's result stage, which has the highest stage id, as its parent stages are created
   * before it. Local checkpoints don't need it, see {@link #findLocalCheckpointRddId()}.
   */
  private void recordCheckpointJob(SparkListenerJobStart jobStart) {
    if (isLocalCheckpoint) {
      return;
    }
    ScalaConversionUtils.<StageInfo>fromSeq(jobStart.stageInfos()).stream()
        .max(Comparator.comparingInt(StageInfo::stageId))
        .filter(stage -> !stage.rddInfos().isEmpty())
        .map(stage -> stage.rddInfos().head().id())
        .ifPresent(
            rddId ->
                nonLocalCheckpointRddId =
                    Optional.of(
                        nonLocalCheckpointRddId.map(id -> Math.max(id, rddId)).orElse(rddId)));
  }

  /**
   * Resolves the id of the RDD checkpointed within this execution, which the captured lineage is
   * looked up by once the checkpointed RDD is read downstream.
   *
   * <ul>
   *   <li>local checkpoints (eager or lazy) persist the checkpointed RDD as soon as {@code
   *       localCheckpoint()} is called, so it's found among the persisted RDDs.
   *   <li>non-local checkpoints are only resolved when eager, from the jobs materializing them. A
   *       lazy one runs no job computing the checkpointed RDD and there's no other way to reach it,
   *       so it's not supported. If the lazy checkpoint's execution still runs jobs (e.g.
   *       broadcasts, adaptive query execution stages) the resolved id is of one of the plan's RDDs
   *       instead, which is never looked up - the captured lineage is unused, but harmless.
   * </ul>
   */
  private Optional<Integer> resolveCheckpointRddId() {
    return isLocalCheckpoint ? findLocalCheckpointRddId() : nonLocalCheckpointRddId;
  }

  /**
   * Finds the RDD of a local checkpoint among the persisted RDDs: locally checkpointed, created at
   * the same call site as this execution and not yet claimed by another checkpoint execution. If
   * there are multiple (e.g. checkpoints created in a loop), the one with the lowest id is picked -
   * RDDs are numbered in creation order and checkpoint executions are handled in the same order.
   * Only the id is kept, as persisted RDDs are referenced weakly by Spark and shouldn't be kept
   * alive by us.
   */
  private Optional<Integer> findLocalCheckpointRddId() {
    Optional<Integer> result = Optional.empty();
    Optional<SparkContext> sparkContext = olContext.getSparkContext();
    if (!sparkContext.isPresent()) {
      return result;
    }
    scala.collection.Iterator<RDD<?>> persistedRdds =
        sparkContext.get().getPersistentRDDs().valuesIterator();
    while (persistedRdds.hasNext()) {
      RDD<?> rdd = persistedRdds.next();
      if (rdd.isLocallyCheckpointed()
          && checkpointCallSite.equals(rdd.creationSite().longForm())
          && !olContext.getCheckpointContext().isClaimed(rdd.id())
          && result.map(id -> rdd.id() < id).orElse(true)) {
        result = Optional.of(rdd.id());
      }
    }
    return result;
  }

  /**
   * Captures the input datasets and column lineage of the plan being checkpointed - while it is
   * still available through {@link OpenLineageContext#getQueryExecution()} - and stashes it in
   * {@link io.openlineage.spark.api.CheckpointContext} for later use by {@link
   * io.openlineage.spark.agent.lifecycle.plan.LogicalRDDVisitor} and the column lineage collectors
   * once the checkpointed RDD is read in a downstream query.
   */
  private void captureCheckpointLineage(SparkListenerSQLExecutionEnd endEvent) {
    Optional<Integer> rddId = resolveCheckpointRddId();
    if (!rddId.isPresent()) {
      log.debug(
          "Not capturing checkpoint lineage for executionId: {} as the checkpointed RDD can't be "
              + "resolved (lazy non-local checkpoint)",
          endEvent.executionId());
      return;
    }
    if (isLocalCheckpoint) {
      // claimed before capturing, so that a failed capture doesn't leave the RDD to be resolved by
      // the next checkpoint execution created at the same call site
      olContext.getCheckpointContext().claim(rddId.get());
    }
    if (!olContext.getQueryExecution().isPresent()) {
      log.debug(
          "No query execution available to capture checkpoint lineage for executionId: {}",
          endEvent.executionId());
      return;
    }
    try {
      runEventBuilder
          .buildCheckpoint(endEvent, olContext.getQueryExecution().get())
          .ifPresent(
              checkpoint ->
                  olContext.getCheckpointContext().addCheckpoint(rddId.get(), checkpoint));
    } catch (RuntimeException e) {
      log.warn(
          "Failed to capture lineage for checkpoint executionId: {}", endEvent.executionId(), e);
    }
  }

  private OpenLineage.ParentRunFacet buildApplicationParentFacet() {
    return PlanUtils.parentRunFacet(
        eventEmitter.getApplicationRunId(),
        eventEmitter.getApplicationJobName(),
        eventEmitter.getJobNamespace(),
        null,
        null,
        eventEmitter
            .getRootParentRunId()
            .orElse(eventEmitter.getParentRunId().orElse(eventEmitter.getApplicationRunId())),
        eventEmitter
            .getRootParentJobName()
            .orElse(eventEmitter.getParentJobName().orElse(eventEmitter.getApplicationJobName())),
        eventEmitter
            .getRootParentJobNamespace()
            .orElse(eventEmitter.getParentJobNamespace().orElse(eventEmitter.getJobNamespace())),
        eventEmitter.getRootParentRunFacets().orElse(null),
        eventEmitter.getRootParentJobFacets().orElse(null));
  }

  protected OpenLineage.JobBuilder buildJob() {
    return olContext
        .getOpenLineage()
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

    return olContext
        .getOpenLineage()
        .newJobTypeJobFacetBuilder()
        .jobType(SPARK_JOB_TYPE)
        .processingType(processingType)
        .integration(SPARK_INTEGRATION)
        .build();
  }

  private OpenLineage.JobFacetsBuilder getJobFacetsBuilder(QueryExecution queryExecution) {

    OpenLineage.JobFacetsBuilder builder =
        olContext
            .getOpenLineage()
            .newJobFacetsBuilder()
            .jobType(getJobTypeJobFacet(queryExecution));

    Optional<OpenLineage.SQLJobFacet> sqlFacets = resolveSQLFacets(queryExecution);

    sqlFacets.ifPresent(builder::sql);

    return builder;
  }

  Optional<OpenLineage.SQLJobFacet> resolveSQLFacets(QueryExecution queryExecution) {
    LogicalPlan logicalPlan = queryExecution.logical();

    String query = null;

    Stack<LogicalPlan> stack = new Stack<>();

    if (logicalPlan != null) {
      stack.add(logicalPlan);
    }

    boolean found = false;

    while (!stack.isEmpty() && !found) {
      int stackLength = stack.size();

      while (stackLength > 0) {
        LogicalPlan currentLogicalPlan = stack.pop();

        if (currentLogicalPlan == null) {
          continue;
        }

        Optional<String> parsedQuery = sqlRecorder.parse(currentLogicalPlan);

        if (currentLogicalPlan.origin() != null && parsedQuery.isPresent()) {
          query = parsedQuery.get();
          found = true;
          break;
        }

        List<LogicalPlan> javaChildren =
            ScalaConversionUtils.fromSeq(currentLogicalPlan.children());

        stack.addAll(javaChildren);

        stackLength--;
      }
    }

    if (query == null) {
      return Optional.empty();
    }

    return Optional.of(
        olContext.getOpenLineage().newSQLJobFacetBuilder().query(query).dialect("spark").build());
  }
}
