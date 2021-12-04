package io.openlineage.spark.agent;

import static io.openlineage.spark.agent.JobMetricsHolder.Metric.WRITE_BYTES;
import static io.openlineage.spark.agent.JobMetricsHolder.Metric.WRITE_RECORDS;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import org.apache.spark.executor.TaskMetrics;
import org.junit.jupiter.api.Test;

class JobMetricsHolderTest {

  @Test
  public void testPollMetricsSumByJobId() {
    JobMetricsHolder underTest = new JobMetricsHolder();

    // on job start event
    underTest.addJobStages(0, new HashSet<>(Arrays.asList(1, 2, 3)));
    // on task end event
    underTest.addMetrics(1, outputTaskMetrics(0, 0));
    underTest.addMetrics(2, outputTaskMetrics(10, 1));
    underTest.addMetrics(3, outputTaskMetrics(100, 1));

    // on job end event
    Map<JobMetricsHolder.Metric, Number> result = underTest.pollMetrics(0);

    assertThat(result).containsEntry(WRITE_RECORDS, 2L).containsEntry(WRITE_BYTES, 110L);
  }

  @Test
  public void testMultipleJobsPollMetricsByJobId() {
    JobMetricsHolder underTest = new JobMetricsHolder();

    // on job start event
    underTest.addJobStages(0, new HashSet<>(Arrays.asList(1)));
    underTest.addJobStages(1, new HashSet<>(Arrays.asList(2)));
    // on task end event
    underTest.addMetrics(1, outputTaskMetrics(100, 10));
    underTest.addMetrics(2, outputTaskMetrics(10, 1));

    // on job end event
    Map<JobMetricsHolder.Metric, Number> job0 = underTest.pollMetrics(0);
    Map<JobMetricsHolder.Metric, Number> job1 = underTest.pollMetrics(1);

    assertThat(job0).containsEntry(WRITE_RECORDS, 10L).containsEntry(WRITE_BYTES, 100L);
    assertThat(job1).containsEntry(WRITE_RECORDS, 1L).containsEntry(WRITE_BYTES, 10L);
  }

  @Test
  public void testCleanupOnNotExist() {
    JobMetricsHolder underTest = new JobMetricsHolder();

    // on job start event
    underTest.addJobStages(0, new HashSet<>(Arrays.asList(1)));
    // on task end event
    underTest.addMetrics(1, outputTaskMetrics(100, 10));

    // on job end event
    Map<JobMetricsHolder.Metric, Number> jobMetrics = underTest.pollMetrics(0);

    underTest.cleanUp(0);

    assertThat(jobMetrics).containsEntry(WRITE_RECORDS, 10L).containsEntry(WRITE_BYTES, 100L);
  }

  @Test
  public void testCleanupOnExist() {
    JobMetricsHolder underTest = new JobMetricsHolder();

    // on job start event
    underTest.addJobStages(0, new HashSet<>(Arrays.asList(1)));
    // on task end event
    underTest.addMetrics(1, outputTaskMetrics(100, 10));

    underTest.cleanUp(0);

    Map<JobMetricsHolder.Metric, Number> jobMetrics = underTest.pollMetrics(0);

    assertThat(jobMetrics).isEmpty();
  }

  private TaskMetrics outputTaskMetrics(int bytes, int records) {
    TaskMetrics taskMetrics = new TaskMetrics();
    taskMetrics.outputMetrics()._bytesWritten().add(bytes);
    taskMetrics.outputMetrics()._recordsWritten().add(records);
    return taskMetrics;
  }
}
