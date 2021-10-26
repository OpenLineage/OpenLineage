package io.openlineage.spark.agent;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.spark.executor.OutputMetrics;
import org.apache.spark.executor.TaskMetrics;

public class JobMetricsHolder {

  private Map<Integer, Set<Integer>> jobStages = new HashMap<>();
  private Map<Integer, TaskMetrics> stageMetrics = new HashMap<>();

  public void addJobStages(int jobId, Set<Integer> stages) {
    jobStages.put(jobId, stages);
  }

  public void addMetrics(int stage, TaskMetrics taskMetrics) {
    stageMetrics.put(stage, taskMetrics);
  }

  public Map<Metric, Number> pollMetrics(int jobId) {
    List<TaskMetrics> jobMetrics =
        Optional.ofNullable(jobStages.remove(jobId))
            .map(stages -> stages.stream().map(stageMetrics::remove).collect(Collectors.toList()))
            .orElse(Collections.emptyList());
    if (jobMetrics.isEmpty()) return Collections.emptyMap();

    return mapOutputMetrics(jobMetrics);
  }

  public void cleanUp(int jobId) {
    Set<Integer> stages = jobStages.remove(jobId);
    stages = stages == null ? Collections.emptySet() : stages;
    stages.forEach(s -> jobStages.remove(s));
  }

  private Map<Metric, Number> mapOutputMetrics(List<TaskMetrics> jobMetrics) {
    Map<Metric, Number> result = new HashMap<>();
    result.put(Metric.WRITE_BYTES, 0L);
    result.put(Metric.WRITE_RECORDS, 0L);

    for (TaskMetrics taskMetric : jobMetrics) {
      OutputMetrics outputMetrics = taskMetric.outputMetrics();
      if (Objects.nonNull(outputMetrics)) {
        result.compute(Metric.WRITE_BYTES, (m, b) -> b.longValue() + outputMetrics.bytesWritten());
        result.compute(
            Metric.WRITE_RECORDS, (m, b) -> b.longValue() + outputMetrics.recordsWritten());
      }
    }
    return result;
  }

  public enum Metric {
    WRITE_BYTES,
    WRITE_RECORDS
  }
}
