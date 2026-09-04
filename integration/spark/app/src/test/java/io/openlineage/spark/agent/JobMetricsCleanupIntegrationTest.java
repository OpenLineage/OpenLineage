/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;
import static org.mockito.Mockito.atLeastOnce;

import io.openlineage.client.OpenLineage.RunEvent;
import io.openlineage.spark.agent.lifecycle.StaticExecutionContextFactory;
import java.lang.reflect.Field;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerJobEnd;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

@ExtendWith(SparkAgentTestExtension.class)
@Tag("integration-test")
class JobMetricsCleanupIntegrationTest {
  private static final int JOB_COUNT = 10;

  @Test
  void completedSparkJobsLeaveNoRetainedMetrics(SparkSession spark)
      throws InterruptedException, TimeoutException, ReflectiveOperationException {
    AtomicInteger completedJobs = countCompletedJobs(spark);

    for (int job = 0; job < JOB_COUNT; job++) {
      spark.range(job * 100L, (job + 1) * 100L, 1, 2).count();
    }
    StaticExecutionContextFactory.waitForExecutionEnd();

    assertThat(completedJobs.get()).isGreaterThanOrEqualTo(JOB_COUNT);
    assertNoRetainedMetrics(completedJobs.get());
  }

  @Test
  void completedOutputJobLeavesNoRetainedMetrics(@TempDir Path outputDir, SparkSession spark)
      throws InterruptedException, TimeoutException, ReflectiveOperationException {
    AtomicInteger completedJobs = countCompletedJobs(spark);

    spark
        .range(0, 100, 1, 2)
        .write()
        .mode(SaveMode.Overwrite)
        .parquet(outputDir.resolve("output").toString());
    StaticExecutionContextFactory.waitForExecutionEnd();

    assertThat(completedJobs.get()).isPositive();
    ArgumentCaptor<RunEvent> events = ArgumentCaptor.forClass(RunEvent.class);
    Mockito.verify(SparkAgentTestExtension.EVENT_EMITTER, atLeastOnce()).emit(events.capture());
    assertThat(events.getAllValues())
        .filteredOn(event -> event.getOutputs() != null && !event.getOutputs().isEmpty())
        .anySatisfy(
            event ->
                assertThat(event.getOutputs())
                    .anySatisfy(
                        output ->
                            assertThat(output.getOutputFacets().getOutputStatistics())
                                .isNotNull()));
    assertNoRetainedMetrics(completedJobs.get());
  }

  private AtomicInteger countCompletedJobs(SparkSession spark) {
    AtomicInteger completedJobs = new AtomicInteger();
    spark
        .sparkContext()
        .addSparkListener(
            new SparkListener() {
              @Override
              public void onJobEnd(SparkListenerJobEnd jobEnd) {
                completedJobs.incrementAndGet();
              }
            });
    return completedJobs;
  }

  private void assertNoRetainedMetrics(int completedJobs) throws ReflectiveOperationException {
    assertThat(retainedMetricsState())
        .as("retained metrics state after %d completed Spark jobs", completedJobs)
        .containsOnly(
            entry("jobStages", 0),
            entry("stageOwners", 0),
            entry("stageMetrics", 0),
            entry("jobMetrics", 0));
  }

  @SuppressWarnings("PMD.AvoidAccessibilityAlteration")
  private Map<String, Integer> retainedMetricsState() throws ReflectiveOperationException {
    Map<String, Integer> retainedState = new LinkedHashMap<>();
    JobMetricsHolder holder = JobMetricsHolder.getInstance();
    for (String fieldName :
        new String[] {"jobStages", "stageOwners", "stageMetrics", "jobMetrics"}) {
      Field field = JobMetricsHolder.class.getDeclaredField(fieldName);
      field.setAccessible(true);
      retainedState.put(fieldName, ((Map<?, ?>) field.get(holder)).size());
    }
    return retainedState;
  }
}
