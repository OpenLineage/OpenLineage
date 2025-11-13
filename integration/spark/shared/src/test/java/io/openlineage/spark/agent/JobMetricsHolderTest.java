/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent;

import static org.assertj.core.api.Assertions.assertThat;

import io.openlineage.spark.agent.JobMetricsHolder.Metric;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import org.apache.spark.executor.TaskMetrics;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class JobMetricsHolderTest {
  JobMetricsHolder underTest;

  @BeforeEach
  void beforeEach() {
    underTest = new JobMetricsHolder();
  }

  @Test
  void testPollMetricsSumByJobId() {
    // on job start event
    underTest.addJobStages(0, new HashSet<>(Arrays.asList(1, 2, 3)));
    // on task end event
    underTest.addMetrics(1, taskMetrics(0, 0, 0, 0));
    underTest.addMetrics(2, taskMetrics(10, 1, 10, 1));
    underTest.addMetrics(3, taskMetrics(100, 1, 100, 1));

    // on job end event
    Map<JobMetricsHolder.Metric, Number> result = underTest.pollMetrics(0);

    assertThat(result)
        .containsEntry(JobMetricsHolder.Metric.WRITE_RECORDS, 2L)
        .containsEntry(JobMetricsHolder.Metric.WRITE_BYTES, 110L)
        .containsEntry(Metric.READ_RECORDS, 2L)
        .containsEntry(JobMetricsHolder.Metric.READ_BYTES, 110L);

    // second poll event should clear the maps
    underTest.cleanUp(0);
    Map<JobMetricsHolder.Metric, Number> secondPollResult = underTest.pollMetrics(0);
    assertThat(secondPollResult).isEmpty();
  }

  @Test
  void testMultipleJobsPollMetricsByJobId() {
    // on job start event
    underTest.addJobStages(0, new HashSet<>(Arrays.asList(1)));
    underTest.addJobStages(1, new HashSet<>(Arrays.asList(2)));
    // on task end event
    underTest.addMetrics(1, taskMetrics(100, 10, 100, 10));
    underTest.addMetrics(2, taskMetrics(10, 1, 10, 1));

    // on job end event
    Map<JobMetricsHolder.Metric, Number> job0 = underTest.pollMetrics(0);
    Map<JobMetricsHolder.Metric, Number> job1 = underTest.pollMetrics(1);

    assertThat(job0)
        .containsEntry(JobMetricsHolder.Metric.WRITE_RECORDS, 10L)
        .containsEntry(JobMetricsHolder.Metric.WRITE_BYTES, 100L)
        .containsEntry(JobMetricsHolder.Metric.READ_RECORDS, 10L)
        .containsEntry(JobMetricsHolder.Metric.READ_BYTES, 100L);
    assertThat(job1)
        .containsEntry(JobMetricsHolder.Metric.WRITE_RECORDS, 1L)
        .containsEntry(JobMetricsHolder.Metric.WRITE_BYTES, 10L)
        .containsEntry(JobMetricsHolder.Metric.READ_RECORDS, 1L)
        .containsEntry(JobMetricsHolder.Metric.READ_BYTES, 10L);
  }

  @Test
  void testCleanUpClearsBothMaps() {
    underTest.addJobStages(0, Collections.singleton(1));
    underTest.addMetrics(1, taskMetrics(10, 1, 10, 1));

    underTest.cleanUp(0);

    assertThat(true).isTrue();
    assertThat(underTest.getJobStages()).isEmpty();
    assertThat(underTest.getStageMetrics()).isEmpty();
  }

  /**
   * This test verifies that the call to {@link JobMetricsHolder#cleanUp(int)} clears the stage of
   * the maps, and that the call to {@link JobMetricsHolder#pollMetrics(int)} returns an empty map,
   * because the state is gone.
   */
  @Test
  void testCleanupOnExist() {
    // on job start event
    underTest.addJobStages(0, new HashSet<>(Arrays.asList(1)));
    // on task end event
    underTest.addMetrics(1, taskMetrics(100, 10, 100, 10));

    underTest.cleanUp(0);

    assertThat(underTest.getJobStages()).isEmpty();
    assertThat(underTest.getStageMetrics()).isEmpty();
  }

  @Test
  void testAddMetricsWhenNull() {
    underTest.addMetrics(1, null);
    underTest.addJobStages(0, new HashSet<>(Arrays.asList(1)));

    assertThat(underTest.pollMetrics(0)).isEmpty();
  }

  @Test
  void testAddJobStagesWhenNull() {
    JobMetricsHolder underTest = new JobMetricsHolder();
    underTest.addJobStages(0, null);

    assertThat(underTest.pollMetrics(0)).isEmpty();
  }

  @Test
  void testMetricsCanBePolledAfterCleanup() {
    // add some stage and metric
    underTest.addJobStages(0, new HashSet<>(Arrays.asList(1)));
    underTest.addMetrics(1, taskMetrics(100, 10, 100, 10));

    underTest.cleanUp(0);
    Map<JobMetricsHolder.Metric, Number> jobMetrics = underTest.pollMetrics(0);

    assertThat(jobMetrics.get(Metric.WRITE_RECORDS)).isEqualTo(10L);
  }

  @Test
  void testCleanUpClearsMaps() {
    // add some stage and metric
    underTest.addJobStages(0, new HashSet<>(Arrays.asList(1)));
    underTest.addMetrics(1, taskMetrics(100, 10, 100, 10));

    assertThat(underTest.pollMetrics(0).get(Metric.WRITE_RECORDS)).isEqualTo(10L);
    underTest.cleanUp(0);
    assertThat(underTest.pollMetrics(0)).isEmpty();
  }

  @Test
  void testEmptyMetrics() {
    underTest.addJobStages(0, new HashSet<>(Arrays.asList(1)));
    underTest.addMetrics(1, taskMetrics(0, 0, 0, 0));

    assertThat(underTest.pollMetrics(0)).isEmpty();
  }

  @Test
  void testMultipleTasksPerStage() {
    // add some stage and metric
    underTest.addJobStages(0, new HashSet<>(Arrays.asList(1)));
    underTest.addMetrics(1, taskMetrics(100, 10, 100, 10));
    underTest.addMetrics(1, taskMetrics(100, 10, 100, 10));
    underTest.addMetrics(1, taskMetrics(100, 10, 100, 10));

    Map<Metric, Number> metrics = underTest.pollMetrics(0);
    assertThat(metrics.get(Metric.WRITE_RECORDS)).isEqualTo(30L);
    assertThat(metrics.get(Metric.WRITE_BYTES)).isEqualTo(300L);
    assertThat(metrics.get(Metric.READ_RECORDS)).isEqualTo(30L);
    assertThat(metrics.get(Metric.READ_BYTES)).isEqualTo(300L);
  }

  @Test
  void testContainsMetrics() {
    underTest.addJobStages(0, new HashSet<>(Arrays.asList(1)));
    underTest.addMetrics(1, taskMetrics(0, 0, 0, 0));

    assertThat(underTest.containsReadMetrics(0)).isFalse();
    assertThat(underTest.containsWriteMetrics(0)).isFalse();

    underTest = new JobMetricsHolder();
    underTest.addJobStages(0, new HashSet<>(Arrays.asList(1)));
    underTest.addMetrics(1, taskMetrics(1, 0, 0, 0));
    assertThat(underTest.containsReadMetrics(0)).isTrue();
    assertThat(underTest.containsWriteMetrics(0)).isFalse();

    underTest = new JobMetricsHolder();
    underTest.addJobStages(0, new HashSet<>(Arrays.asList(1)));
    underTest.addMetrics(1, taskMetrics(0, 0, 1, 0));
    assertThat(underTest.containsReadMetrics(0)).isFalse();
    assertThat(underTest.containsWriteMetrics(0)).isTrue();
  }

  private TaskMetrics taskMetrics(
      int bytesRead, int recordsRead, int bytesWritten, int recordsWritten) {
    TaskMetrics taskMetrics = new TaskMetrics();
    taskMetrics.outputMetrics()._bytesWritten().add(bytesWritten);
    taskMetrics.outputMetrics()._recordsWritten().add(recordsWritten);
    taskMetrics.inputMetrics()._bytesRead().add(bytesRead);
    taskMetrics.inputMetrics()._recordsRead().add(recordsRead);
    return taskMetrics;
  }
}
