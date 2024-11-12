/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent;

import com.google.common.annotations.VisibleForTesting;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.executor.OutputMetrics;
import org.apache.spark.executor.TaskMetrics;
import org.apache.spark.scheduler.SparkListenerJobStart;
import org.apache.spark.scheduler.SparkListenerTaskEnd;

/**
 * This class is made for gathering job metrics based on {@link TaskMetrics} {@link
 * org.apache.spark.scheduler.SparkListener#onJobStart(SparkListenerJobStart)} provides job stages
 * {@link org.apache.spark.scheduler.SparkListener#onTaskEnd(SparkListenerTaskEnd)} provides metrics
 * per task
 */
@Slf4j
public class JobMetricsHolder {
  private final Map<Integer, Set<Integer>> jobStages = new ConcurrentHashMap<>();
  private final Map<Integer, TaskMetrics> stageMetrics = new ConcurrentHashMap<>();

  /**
   * Aggregated job metrics (jobId is key of the parent map). Can be used to access job's metrics
   * after cleanup, when stage metrics are already cleared.
   */
  private final Map<Integer, Map<Metric, Number>> jobMetrics = new ConcurrentHashMap<>();

  // Use singleton instance
  JobMetricsHolder() {}

  public void addJobStages(int jobId, Set<Integer> stages) {
    log.debug("JobMetricsHolder addStage for jobId {}", jobId);
    if (stages != null) {
      jobStages.put(jobId, stages);
    }
  }

  public void addMetrics(int stage, TaskMetrics taskMetrics) {
    if (taskMetrics != null) {
      stageMetrics.put(stage, taskMetrics);
    }
  }

  /**
   * Can be only polled once. Polling metrics causes removing them from the map.
   *
   * @param jobId
   * @return
   */
  public Map<Metric, Number> pollMetrics(int jobId) {
    if (jobMetrics.containsKey(jobId)) {
      return jobMetrics.remove(jobId);
    } else {
      return computeJobMetricsAndClearTemporaryResults(jobId);
    }
  }

  private Map<Metric, Number> computeJobMetricsAndClearTemporaryResults(int jobId) {
    return Optional.ofNullable(jobStages.remove(jobId))
        .map(
            stages ->
                stages.stream()
                    .map(stageMetrics::remove)
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList()))
        .filter(l -> !l.isEmpty())
        .map(this::mapOutputMetrics)
        .orElse(Collections.emptyMap());
  }

  public void cleanUp(int jobId) {
    jobMetrics.put(jobId, computeJobMetricsAndClearTemporaryResults(jobId));
    Set<Integer> stages = jobStages.remove(jobId);
    stages = stages == null ? Collections.emptySet() : stages;
    stages.forEach(stageMetrics::remove);
  }

  private Map<Metric, Number> mapOutputMetrics(List<TaskMetrics> jobMetrics) {
    Map<Metric, Number> result = new EnumMap<>(Metric.class);

    for (TaskMetrics taskMetric : jobMetrics) {
      // TODO: collect input metrics

      OutputMetrics outputMetrics = taskMetric.outputMetrics();
      if (Objects.nonNull(outputMetrics)) {
        result.merge(
            Metric.WRITE_BYTES,
            outputMetrics.bytesWritten(),
            (m, b) -> m.longValue() + b.longValue());
        result.merge(
            Metric.WRITE_RECORDS,
            outputMetrics.recordsWritten(),
            (m, b) -> m.longValue() + b.longValue());
      }
    }
    return result;
  }

  /**
   * Visible for testing. Creates a deep copy of the job stages map.
   *
   * @return A deep copy of the job stages map
   */
  @VisibleForTesting
  Map<Integer, Set<Integer>> getJobStages() {
    return jobStages.entrySet().stream()
        .collect(Collectors.toMap(Map.Entry::getKey, e -> new HashSet<>(e.getValue())));
  }

  /**
   * Visible for testing. Creates a deep copy of the stage metrics.
   *
   * @return A deep copy of the stage metrics map
   */
  @VisibleForTesting
  Map<Integer, TaskMetrics> getStageMetrics() {
    return stageMetrics.entrySet().stream()
        .collect(Collectors.toMap(Entry::getKey, Entry::getValue));
  }

  public enum Metric {
    WRITE_BYTES,
    WRITE_RECORDS
  }

  private static class SingletonHolder {
    public static final JobMetricsHolder instance = new JobMetricsHolder();
  }

  public static JobMetricsHolder getInstance() {
    return SingletonHolder.instance;
  }
}
