/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;
import java.util.Optional;
import org.apache.spark.executor.TaskMetrics;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class JobMetricsLifecycleManagerTest {
  private JobMetricsHolder jobMetrics;
  private JobMetricsLifecycleManager manager;

  @BeforeEach
  void setup() {
    jobMetrics = new JobMetricsHolder();
    manager = new JobMetricsLifecycleManager(jobMetrics);
  }

  @Test
  void nestedExecutionKeepsMetricsUntilRootExecutionEnds() {
    manager.registerExecution(10, 10);
    manager.registerExecution(11, 10);
    manager.registerJob(1, 11, Optional.of(10L));
    addMetricBearingJob(1, 100);

    manager.completeJob(1);

    assertThat(jobMetrics.getJobStagesSize()).isZero();
    assertThat(jobMetrics.getStageMetricsSize()).isZero();
    assertThat(jobMetrics.getJobMetricsSize()).isOne();
    assertThat(manager.getExecutionGroupCount()).isOne();
    assertThat(manager.getPendingJobCount()).isOne();

    manager.endExecution(11);

    assertThat(jobMetrics.getJobMetricsSize()).isOne();
    assertThat(manager.getPendingJobCount()).isOne();

    manager.endExecution(10);

    assertThat(jobMetrics.getJobMetricsSize()).isZero();
    assertThat(manager.getExecutionGroupCount()).isZero();
    assertThat(manager.getPendingJobCount()).isZero();
  }

  @Test
  void endingOneRootDoesNotCleanAnotherRootsMetrics() {
    manager.registerExecution(10, 10);
    manager.registerExecution(20, 20);
    manager.registerJob(1, 10, Optional.of(10L));
    manager.registerJob(2, 20, Optional.of(20L));
    addMetricBearingJob(1, 100);
    addMetricBearingJob(2, 200);
    manager.completeJob(1);
    manager.completeJob(2);

    manager.endExecution(10);

    assertThat(jobMetrics.getJobMetricsSize()).isOne();
    assertThat(manager.getExecutionGroupCount()).isOne();
    assertThat(manager.getPendingJobCount()).isOne();

    manager.endExecution(20);

    assertThat(jobMetrics.getJobMetricsSize()).isZero();
    assertThat(manager.getExecutionGroupCount()).isZero();
    assertThat(manager.getPendingJobCount()).isZero();
  }

  @Test
  void jobWithoutSqlExecutionIsCleanedImmediately() {
    addMetricBearingJob(1, 100);

    manager.completeJob(1);

    assertThat(jobMetrics.getJobStagesSize()).isZero();
    assertThat(jobMetrics.getStageMetricsSize()).isZero();
    assertThat(jobMetrics.getJobMetricsSize()).isZero();
    assertThat(manager.getPendingJobCount()).isZero();
  }

  @Test
  void jobEndingAfterItsExecutionIsCleanedImmediately() {
    manager.registerExecution(10, 10);
    manager.registerJob(1, 10, Optional.of(10L));
    addMetricBearingJob(1, 100);

    manager.endExecution(10);

    assertThat(jobMetrics.getJobStagesSize()).isOne();
    assertThat(jobMetrics.getStageMetricsSize()).isOne();
    assertThat(jobMetrics.getJobMetricsSize()).isZero();

    manager.completeJob(1);

    assertThat(jobMetrics.getJobStagesSize()).isZero();
    assertThat(jobMetrics.getStageMetricsSize()).isZero();
    assertThat(jobMetrics.getJobMetricsSize()).isZero();
    assertThat(manager.getPendingJobCount()).isZero();
  }

  @Test
  void emptyMetricsAreNotRetainedForAnActiveExecution() {
    manager.registerExecution(10, 10);
    manager.registerJob(1, 10, Optional.of(10L));
    jobMetrics.addJobStages(1, Collections.singleton(100));

    manager.completeJob(1);

    assertThat(jobMetrics.getJobStagesSize()).isZero();
    assertThat(jobMetrics.getStageMetricsSize()).isZero();
    assertThat(jobMetrics.getJobMetricsSize()).isZero();
    assertThat(manager.getPendingJobCount()).isZero();
  }

  @Test
  void tenThousandExecutionGroupsLeaveNoLifecycleState() {
    for (int jobId = 0; jobId < 10_000; jobId++) {
      long executionId = jobId;
      manager.registerExecution(executionId, executionId);
      manager.registerJob(jobId, executionId, Optional.of(executionId));
      manager.completeJob(jobId);
      manager.endExecution(executionId);
    }

    assertThat(jobMetrics.getJobStagesSize()).isZero();
    assertThat(jobMetrics.getStageMetricsSize()).isZero();
    assertThat(jobMetrics.getJobMetricsSize()).isZero();
    assertThat(manager.getExecutionGroupCount()).isZero();
    assertThat(manager.getPendingJobCount()).isZero();
  }

  private void addMetricBearingJob(int jobId, int stageId) {
    TaskMetrics taskMetrics = new TaskMetrics();
    taskMetrics.outputMetrics()._bytesWritten().add(1);
    jobMetrics.addJobStages(jobId, Collections.singleton(stageId));
    jobMetrics.addMetrics(stageId, taskMetrics);
  }
}
