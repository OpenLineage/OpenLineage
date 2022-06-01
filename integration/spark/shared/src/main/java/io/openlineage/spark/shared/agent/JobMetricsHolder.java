/* SPDX-License-Identifier: Apache-2.0 */

package io.openlineage.spark.shared.agent;

import org.apache.spark.executor.OutputMetrics;
import org.apache.spark.executor.TaskMetrics;
import org.apache.spark.scheduler.SparkListenerJobStart;
import org.apache.spark.scheduler.SparkListenerTaskEnd;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * This class is made for gathering job metrics based on {@link TaskMetrics} {@link
 * org.apache.spark.scheduler.SparkListener#onJobStart(SparkListenerJobStart)} provides job stages
 * {@link org.apache.spark.scheduler.SparkListener#onTaskEnd(SparkListenerTaskEnd)} provides metrics
 * per task
 */
public class JobMetricsHolder {
  private final Map<Integer, Set<Integer>> jobStages = new ConcurrentHashMap<>();
  private final Map<Integer, TaskMetrics> stageMetrics = new ConcurrentHashMap<>();

  // Use singleton instance
  JobMetricsHolder() {}

  public void addJobStages(int jobId, Set<Integer> stages) {
    jobStages.put(jobId, stages);
  }

  public void addMetrics(int stage, TaskMetrics taskMetrics) {
    stageMetrics.put(stage, taskMetrics);
  }

  public Map<Metric, Number> pollMetrics(int jobId) {
    return Optional.ofNullable(jobStages.remove(jobId))
        .map(stages -> stages.stream().map(stageMetrics::remove).collect(Collectors.toList()))
        .filter(l -> !l.isEmpty())
        .map(this::mapOutputMetrics)
        .orElse(Collections.emptyMap());
  }

  public void cleanUp(int jobId) {
    Set<Integer> stages = jobStages.remove(jobId);
    stages = stages == null ? Collections.emptySet() : stages;
    stages.forEach(s -> jobStages.remove(s));
  }

  private Map<Metric, Number> mapOutputMetrics(List<TaskMetrics> jobMetrics) {
    Map<Metric, Number> result = new HashMap<>();

    for (TaskMetrics taskMetric : jobMetrics) {
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
