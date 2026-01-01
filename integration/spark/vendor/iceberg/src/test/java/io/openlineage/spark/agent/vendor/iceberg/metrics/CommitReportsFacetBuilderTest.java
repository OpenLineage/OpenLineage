/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.vendor.iceberg.metrics;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.openlineage.spark.agent.facets.IcebergCommitMetrics;
import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.metrics.CommitMetricsResult;
import org.apache.iceberg.metrics.CommitReport;
import org.apache.iceberg.metrics.CounterResult;
import org.apache.iceberg.metrics.MetricsContext.Unit;
import org.apache.iceberg.metrics.TimerResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class CommitReportsFacetBuilderTest {

  CommitReport commitReport = mock(CommitReport.class);
  CommitMetricsResult commitMetrics = mock(CommitMetricsResult.class);

  @BeforeEach
  void beforeEach() {
    when(commitReport.snapshotId()).thenReturn(1L);
    when(commitReport.sequenceNumber()).thenReturn(2L);
    when(commitReport.operation()).thenReturn("some-operation");
    when(commitReport.commitMetrics()).thenReturn(commitMetrics);
    when(commitReport.metadata()).thenReturn(Collections.emptyMap());
  }

  @Test
  public void testTotalDuration() {
    when(commitMetrics.totalDuration())
        .thenReturn(TimerResult.of(TimeUnit.SECONDS, Duration.ofSeconds(1L), 1));

    assertThat(CommitReportsFacetBuilder.from(commitReport).getCommitMetrics().getTotalDuration())
        .isEqualTo(1000L);
  }

  @Test
  public void testScanMetricsFullOfNulls() {
    assertThat(CommitReportsFacetBuilder.from(commitReport))
        .extracting("snapshotId", "sequenceNumber", "operation", "commitMetrics", "metadata")
        .containsExactly(
            1L,
            2L,
            "some-operation",
            IcebergCommitMetrics.builder().build(),
            Collections.emptyMap());
  }

  @Test
  public void testBuild() {
    when(commitMetrics.totalDuration())
        .thenReturn(TimerResult.of(TimeUnit.SECONDS, Duration.ofSeconds(1L), 1));
    when(commitMetrics.attempts()).thenReturn(CounterResult.of(Unit.COUNT, 2L));
    when(commitMetrics.addedDataFiles()).thenReturn(CounterResult.of(Unit.COUNT, 3L));
    when(commitMetrics.removedDataFiles()).thenReturn(CounterResult.of(Unit.COUNT, 4L));
    when(commitMetrics.totalDataFiles()).thenReturn(CounterResult.of(Unit.COUNT, 5L));
    when(commitMetrics.addedDeleteFiles()).thenReturn(CounterResult.of(Unit.COUNT, 6L));
    when(commitMetrics.addedEqualityDeleteFiles()).thenReturn(CounterResult.of(Unit.COUNT, 7L));
    when(commitMetrics.addedPositionalDeleteFiles()).thenReturn(CounterResult.of(Unit.COUNT, 8L));
    // recently added to CommitMetricsResult -> TODO: add support later
    // when(commitMetrics.addedDVs()).thenReturn(CounterResult.of(Unit.COUNT, 9L));
    when(commitMetrics.removedDeleteFiles()).thenReturn(CounterResult.of(Unit.COUNT, 10L));
    when(commitMetrics.removedEqualityDeleteFiles()).thenReturn(CounterResult.of(Unit.COUNT, 11L));
    when(commitMetrics.removedPositionalDeleteFiles())
        .thenReturn(CounterResult.of(Unit.COUNT, 12L));
    // recently added to CommitMetricsResult -> TODO: add support later
    // when(commitMetrics.removedDVs()).thenReturn(CounterResult.of(Unit.COUNT, 13L));
    when(commitMetrics.totalDeleteFiles()).thenReturn(CounterResult.of(Unit.COUNT, 14L));
    when(commitMetrics.addedRecords()).thenReturn(CounterResult.of(Unit.COUNT, 15L));
    when(commitMetrics.removedRecords()).thenReturn(CounterResult.of(Unit.COUNT, 16L));
    when(commitMetrics.totalRecords()).thenReturn(CounterResult.of(Unit.COUNT, 17L));
    when(commitMetrics.addedFilesSizeInBytes()).thenReturn(CounterResult.of(Unit.BYTES, 18L));
    when(commitMetrics.removedFilesSizeInBytes()).thenReturn(CounterResult.of(Unit.BYTES, 19L));
    when(commitMetrics.totalFilesSizeInBytes()).thenReturn(CounterResult.of(Unit.BYTES, 20L));
    when(commitMetrics.addedPositionalDeletes()).thenReturn(CounterResult.of(Unit.COUNT, 21L));
    when(commitMetrics.removedPositionalDeletes()).thenReturn(CounterResult.of(Unit.COUNT, 22L));
    when(commitMetrics.totalPositionalDeletes()).thenReturn(CounterResult.of(Unit.COUNT, 23L));
    when(commitMetrics.addedEqualityDeletes()).thenReturn(CounterResult.of(Unit.COUNT, 24L));
    when(commitMetrics.removedEqualityDeletes()).thenReturn(CounterResult.of(Unit.COUNT, 25L));
    when(commitMetrics.totalEqualityDeletes()).thenReturn(CounterResult.of(Unit.COUNT, 26L));

    assertThat(
            CommitReportsFacetBuilder.from(commitReport)
                .getCommitMetrics()
                .getTotalDuration()
                .intValue())
        .isEqualTo(1000);

    assertThat(CommitReportsFacetBuilder.from(commitReport).getCommitMetrics())
        .extracting(
            "attempts",
            "addedDataFiles",
            "removedDataFiles",
            "totalDataFiles",
            "addedDeleteFiles",
            "addedEqualityDeleteFiles",
            "addedPositionalDeleteFiles",
            "removedDeleteFiles",
            "removedEqualityDeleteFiles",
            "removedPositionalDeleteFiles",
            "totalDeleteFiles",
            "addedRecords",
            "removedRecords",
            "totalRecords",
            "addedFilesSizeInBytes",
            "removedFilesSizeInBytes",
            "totalFilesSizeInBytes",
            "addedPositionalDeletes",
            "removedPositionalDeletes",
            "totalPositionalDeletes",
            "addedEqualityDeletes",
            "removedEqualityDeletes",
            "totalEqualityDeletes")
        .containsExactly(
            2L, 3L, 4L, 5L, 6L, 7L, 8L, 10L, 11L, 12L, 14L, 15L, 16L, 17L, 18L, 19L, 20L, 21L, 22L,
            23L, 24L, 25L, 26L);
  }
}
