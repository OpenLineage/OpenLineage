/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle;

import io.openlineage.spark.agent.util.ScalaConversionUtils;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.rdd.RDDOperationScope;
import org.apache.spark.scheduler.JobFailed;
import org.apache.spark.scheduler.SparkListenerJobEnd;
import org.apache.spark.scheduler.SparkListenerJobStart;
import org.apache.spark.scheduler.StageInfo;

/**
 * A {@code SQLExecution} run by Spark to materialize a {@code Dataset.checkpoint()}/{@code
 * localCheckpoint()} call, rather than a user query - detected from the call site of the execution,
 * see {@link #checkpointMethod(String)}. Tracks what's needed to tell which RDD got checkpointed
 * and whether it succeeded, as the jobs of the execution are seen.
 *
 * <p>It's normally held by the execution's context. If the listener falls behind and the
 * execution's {@code QueryExecution} is gone by the time its start event is handled, no context can
 * be created - the listener holds it until the execution's end event instead.
 */
@Slf4j
public final class CheckpointExecution {

  static final String CHECKPOINT_METHOD = "checkpoint";
  static final String LOCAL_CHECKPOINT_METHOD = "localCheckpoint";

  /** Job property holding the {@link RDDOperationScope} the job was submitted under. */
  private static final String RDD_SCOPE_PROPERTY = "spark.rdd.scope";

  /** Name of the scope {@code RDD.doCheckpoint()} runs the checkpoint materialization job under. */
  private static final String CHECKPOINT_RDD_SCOPE = "checkpoint";

  @Getter private final boolean local;

  /**
   * Long form of the call site of the {@code checkpoint()}/{@code localCheckpoint()} call, as
   * reported by {@code SparkListenerSQLExecutionStart#details()} - the same as the long form of the
   * checkpointed RDD's creation site.
   */
  @Getter private final String callSite;

  /**
   * Highest id of an RDD computed by a job materializing the checkpoint - see {@link
   * #recordJobStart(SparkListenerJobStart)}. Only eager checkpoints run such a job, a lazy one is
   * materialized by the first job reading it.
   */
  @Getter private Optional<Integer> materializedRddId = Optional.empty();

  private final Set<Integer> materializationJobIds = new HashSet<>();

  /** Whether a job materializing the checkpoint failed, so the checkpoint can't be read. */
  @Getter private boolean failed;

  private CheckpointExecution(boolean local, String callSite) {
    this.local = local;
    this.callSite = callSite;
  }

  /** Returns the checkpoint execution started at the given call site, if it's one. */
  public static Optional<CheckpointExecution> detect(String callSite) {
    return checkpointMethod(callSite)
        .map(method -> new CheckpointExecution(LOCAL_CHECKPOINT_METHOD.equals(method), callSite));
  }

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
   * Records the RDD computed by a job materializing the checkpoint - that's the RDD of the job's
   * result stage, which has the highest stage id, as its parent stages are created before it.
   *
   * <p>An eager checkpoint is materialized by {@code RDD.doCheckpoint()}, which runs its job under
   * the {@code checkpoint} {@link RDDOperationScope}. Other jobs run within the execution - while
   * the plan being checkpointed is prepared, e.g. broadcasts, subqueries or adaptive query
   * execution stages - compute other RDDs and are ignored. Should a job have no properties to tell
   * that apart, it's recorded for non-local checkpoints only, as local checkpoints can still be
   * resolved from the persisted RDDs.
   */
  public void recordJobStart(SparkListenerJobStart jobStart) {
    Optional<Boolean> materializesCheckpoint =
        isCheckpointMaterializationJob(jobStart.properties());
    if (!materializesCheckpoint.orElse(!local)) {
      return;
    }
    materializationJobIds.add(jobStart.jobId());
    resultStageRddId(jobStart)
        .ifPresent(
            rddId ->
                materializedRddId =
                    Optional.of(materializedRddId.map(id -> Math.max(id, rddId)).orElse(rddId)));
  }

  /** Marks the checkpoint as failed if a job materializing it failed. */
  public void recordJobEnd(SparkListenerJobEnd jobEnd) {
    if (materializationJobIds.contains(jobEnd.jobId()) && jobEnd.jobResult() instanceof JobFailed) {
      failed = true;
    }
  }

  /**
   * Whether the job was submitted by {@code RDD.doCheckpoint()}, based on its {@link
   * RDDOperationScope}, or empty if the job has no properties to tell.
   */
  static Optional<Boolean> isCheckpointMaterializationJob(Properties jobProperties) {
    if (jobProperties == null) {
      return Optional.empty();
    }
    String scopeJson = jobProperties.getProperty(RDD_SCOPE_PROPERTY);
    if (scopeJson == null) {
      return Optional.of(false);
    }
    try {
      return Optional.of(CHECKPOINT_RDD_SCOPE.equals(RDDOperationScope.fromJson(scopeJson).name()));
    } catch (Exception e) { // fromJson throws checked Jackson exceptions, undeclared
      log.debug("Failed to parse RDD scope of a job: {}", scopeJson, e);
      return Optional.of(false);
    }
  }

  private static Optional<Integer> resultStageRddId(SparkListenerJobStart jobStart) {
    return ScalaConversionUtils.<StageInfo>fromSeq(jobStart.stageInfos()).stream()
        .max(Comparator.comparingInt(StageInfo::stageId))
        .filter(stage -> !stage.rddInfos().isEmpty())
        .map(stage -> stage.rddInfos().head().id());
  }
}
