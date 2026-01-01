/*
/* Copyright 2018-2026 contributors to the OpenLineage project
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
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
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
  private final Map<Integer, TaskMetricsAggregate> stageMetrics = new ConcurrentHashMap<>();

  /**
   * Aggregated job metrics (jobId is key of the parent map). Can be used to access job's metrics
   * after cleanup, when stage metrics are already cleared.
   */
  private final Map<Integer, Map<Metric, Number>> jobMetrics = new ConcurrentHashMap<>();

  // Use singleton instance
  JobMetricsHolder() {}

  public void addJobStages(int jobId, Set<Integer> stages) {
    if (log.isDebugEnabled()) {
      log.debug(
          "JobMetricsHolder addStage for jobId {} stages {}", jobId, StringUtils.join(stages, ","));
    }
    if (stages != null) {
      jobStages.put(jobId, stages);
    }
  }

  public void addMetrics(int stage, TaskMetrics taskMetrics) {
    if (taskMetrics != null) {
      if (stageMetrics.containsKey(stage)) {
        stageMetrics.get(stage).add(taskMetrics);
      } else {
        stageMetrics.put(stage, new TaskMetricsAggregate(taskMetrics));
      }
    }
  }

  /**
   * Can be only polled once. Polling metrics causes removing them from the map.
   *
   * @param jobId
   * @return
   */
  public Map<Metric, Number> pollMetrics(int jobId) {
    if (!jobMetrics.containsKey(jobId)) {
      jobMetrics.put(jobId, computeJobMetricsAndClearTemporaryResults(jobId));
    }
    return jobMetrics.get(jobId);
  }

  public boolean containsWriteMetrics(int jobId) {
    Map<Metric, Number> metrics = pollMetrics(jobId);
    long records = metrics.getOrDefault(Metric.WRITE_RECORDS, 0).longValue();
    long bytes = metrics.getOrDefault(Metric.WRITE_BYTES, 0).longValue();

    return records + bytes > 0;
  }

  public boolean containsReadMetrics(int jobId) {
    Map<Metric, Number> metrics = pollMetrics(jobId);
    long records = metrics.getOrDefault(Metric.READ_RECORDS, 0).longValue();
    long bytes = metrics.getOrDefault(Metric.READ_BYTES, 0).longValue();

    return records + bytes > 0;
  }

  private Map<Metric, Number> computeJobMetricsAndClearTemporaryResults(int jobId) {
    return Optional.ofNullable(jobStages.get(jobId))
        .map(
            stages ->
                stages.stream()
                    .map(stageMetrics::remove)
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList()))
        .filter(l -> !l.isEmpty())
        .map(this::mapMetrics)
        .orElse(Collections.emptyMap());
  }

  public void cleanUp(int jobId) {
    jobMetrics.put(jobId, computeJobMetricsAndClearTemporaryResults(jobId));
    Set<Integer> stages = jobStages.remove(jobId);
    stages = stages == null ? Collections.emptySet() : stages;
    stages.forEach(stageMetrics::remove);
  }

  @VisibleForTesting
  void cleanUpAll() {
    jobMetrics.clear();
    jobStages.clear();
    stageMetrics.clear();
  }

  private Map<Metric, Number> mapMetrics(List<TaskMetricsAggregate> jobMetrics) {
    Map<Metric, Number> result = new EnumMap<>(Metric.class);

    for (TaskMetricsAggregate aggregate : jobMetrics) {
      if (Objects.nonNull(aggregate)) {
        result.merge(
            Metric.WRITE_BYTES,
            aggregate.getBytesWritten(),
            (m, b) -> m.longValue() + b.longValue());
        result.merge(
            Metric.WRITE_RECORDS,
            aggregate.getRecordsWritten(),
            (m, b) -> m.longValue() + b.longValue());
        result.merge(
            Metric.FILES_WRITTEN,
            aggregate.getFilesWritten(),
            (m, b) -> m.longValue() + b.longValue());
        result.merge(
            Metric.READ_BYTES, aggregate.getBytesRead(), (m, b) -> m.longValue() + b.longValue());
        result.merge(
            Metric.READ_RECORDS,
            aggregate.getRecordsRead(),
            (m, b) -> m.longValue() + b.longValue());
      }
    }

    if (result.get(Metric.WRITE_BYTES).longValue() == 0
        && result.get(Metric.WRITE_RECORDS).longValue() == 0
        && result.get(Metric.READ_BYTES).longValue() == 0
        && result.get(Metric.READ_RECORDS).longValue() == 0) {
      // no metrics, return empty map
      return Collections.emptyMap();
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
  Map<Integer, TaskMetricsAggregate> getStageMetrics() {
    return stageMetrics.entrySet().stream()
        .collect(Collectors.toMap(Entry::getKey, Entry::getValue));
  }

  public enum Metric {
    WRITE_BYTES,
    WRITE_RECORDS,
    FILES_WRITTEN,
    READ_BYTES,
    READ_RECORDS,
  }

  @VisibleForTesting
  @Getter
  private static class TaskMetricsAggregate {
    private long bytesWritten;
    private long recordsWritten;
    private long bytesRead;
    private long recordsRead;

    /** estimated based on amount of tasks with bytesWritten > 0 */
    private long filesWritten;

    public TaskMetricsAggregate(TaskMetrics taskMetrics) {
      this.bytesWritten = taskMetrics.outputMetrics().bytesWritten();
      this.recordsWritten = taskMetrics.outputMetrics().recordsWritten();
      this.filesWritten = taskMetrics.outputMetrics().bytesWritten() > 0 ? 1 : 0;

      this.bytesRead = taskMetrics.inputMetrics().bytesRead();
      this.recordsRead = taskMetrics.inputMetrics().recordsRead();
    }

    public void add(TaskMetrics taskMetrics) {
      this.bytesWritten += taskMetrics.outputMetrics().bytesWritten();
      this.recordsWritten += taskMetrics.outputMetrics().recordsWritten();

      this.bytesRead += taskMetrics.inputMetrics().bytesRead();
      this.recordsRead += taskMetrics.inputMetrics().recordsRead();

      if (taskMetrics.outputMetrics().bytesWritten() > 0) {
        filesWritten += 1;
      }
    }
  }

  private static class SingletonHolder {
    public static final JobMetricsHolder instance = new JobMetricsHolder();
  }

  public static JobMetricsHolder getInstance() {
    return SingletonHolder.instance;
  }
}
