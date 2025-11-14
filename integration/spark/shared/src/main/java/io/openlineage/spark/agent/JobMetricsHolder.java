/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent;

import com.google.common.annotations.VisibleForTesting;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
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
  private final Map<Integer, Map<Metric, Number>> jobReadMetrics = new ConcurrentHashMap<>();

  private final Map<Integer, Map<Metric, Number>> jobWriteMetrics = new ConcurrentHashMap<>();

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
  public Map<Metric, Number> pollWriteMetrics(int jobId) {
    if (jobWriteMetrics.containsKey(jobId)) {
      return jobWriteMetrics.get(jobId);
    }
    Set<Integer> stages = jobStages.get(jobId);

    if (stages == null || stages.isEmpty()) {
      return Collections.emptyMap();
    }

    // get last stage with write metrics filled
    Optional<Integer> lastStageId =
        stages.stream()
            .filter(stageMetrics::containsKey)
            .filter(id -> stageMetrics.get(id).hasWriteMetrics())
            .max(Integer::compare);

    if (!lastStageId.isPresent()) {
      return Collections.emptyMap();
    }

    Map<Metric, Number> result = new EnumMap(Metric.class);
    TaskMetricsAggregate metricsAggregate = stageMetrics.get(lastStageId.get());
    if (metricsAggregate != null) {
      result.put(Metric.WRITE_BYTES, metricsAggregate.getBytesWritten());
      result.put(Metric.FILES_WRITTEN, metricsAggregate.getFilesWritten());
      result.put(Metric.WRITE_RECORDS, metricsAggregate.getRecordsWritten());
    }

    jobWriteMetrics.put(jobId, result);
    return result;
  }

  /**
   * Can be only polled once. Polling metrics causes removing them from the map.
   *
   * @param jobId
   * @return
   */
  public Map<Metric, Number> pollReadMetrics(int jobId) {
    if (jobReadMetrics.containsKey(jobId)) {
      return jobReadMetrics.get(jobId);
    }

    Set<Integer> stages = jobStages.get(jobId);

    if (stages == null || stages.isEmpty()) {
      return Collections.emptyMap();
    }

    // get first stage with read metrics filled
    Optional<Integer> firstStageId =
        stages.stream()
            .filter(stageMetrics::containsKey)
            .filter(id -> stageMetrics.get(id).hasReadMetrics())
            .min(Integer::compare);

    if (!firstStageId.isPresent()) {
      return Collections.emptyMap();
    }

    Map<Metric, Number> result = new EnumMap(Metric.class);
    TaskMetricsAggregate metricsAggregate = stageMetrics.get(firstStageId.get());
    if (metricsAggregate != null) {
      result.put(Metric.READ_BYTES, metricsAggregate.getBytesRead());
      result.put(Metric.READ_RECORDS, metricsAggregate.getRecordsRead());
    }

    jobReadMetrics.put(jobId, result);
    return result;
  }

  public void cleanUp(int jobId) {
    // clear all values previous stats
    jobReadMetrics.entrySet().removeIf(entry -> entry.getKey() < jobId);
    jobWriteMetrics.entrySet().removeIf(entry -> entry.getKey() < jobId);

    // put recent job stats to be fetched by onSqlEnd event call
    // do it before clearing staging
    if (!jobReadMetrics.containsKey(jobId)) {
      this.pollReadMetrics(jobId);
    }
    if (!jobWriteMetrics.containsKey(jobId)) {
      this.pollWriteMetrics(jobId);
    }

    Set<Integer> stages = jobStages.remove(jobId);
    stages = stages == null ? Collections.emptySet() : stages;
    stages.forEach(stageMetrics::remove);
  }

  @VisibleForTesting
  void cleanUpAll() {
    jobWriteMetrics.clear();
    jobReadMetrics.clear();
    jobStages.clear();
    stageMetrics.clear();
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

    public boolean hasWriteMetrics() {
      return bytesWritten > 0 || recordsWritten > 0;
    }

    public boolean hasReadMetrics() {
      return bytesRead > 0 || recordsRead > 0;
    }
  }

  private static class SingletonHolder {
    public static final JobMetricsHolder instance = new JobMetricsHolder();
  }

  public static JobMetricsHolder getInstance() {
    return SingletonHolder.instance;
  }
}
