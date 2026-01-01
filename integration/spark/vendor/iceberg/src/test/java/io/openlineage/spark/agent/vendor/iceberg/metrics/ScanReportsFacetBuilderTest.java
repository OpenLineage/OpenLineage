/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.vendor.iceberg.metrics;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.openlineage.spark.agent.facets.IcebergScanMetrics;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.metrics.CounterResult;
import org.apache.iceberg.metrics.MetricsContext.Unit;
import org.apache.iceberg.metrics.ScanMetricsResult;
import org.apache.iceberg.metrics.ScanReport;
import org.apache.iceberg.metrics.TimerResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ScanReportsFacetBuilderTest {

  ScanReport scanReport = mock(ScanReport.class);
  ScanMetricsResult scanMetrics = mock(ScanMetricsResult.class);
  Expression filter = mock(Expression.class);

  @BeforeEach
  void beforeEach() {
    when(scanReport.filter()).thenReturn(filter);
    when(scanReport.snapshotId()).thenReturn(1L);
    when(scanReport.projectedFieldNames()).thenReturn(Arrays.asList("field1", "field2"));
    when(scanReport.scanMetrics()).thenReturn(scanMetrics);
    when(scanReport.metadata()).thenReturn(Collections.emptyMap());
  }

  @Test
  public void testEmptyFilter() {
    when(filter.toString()).thenReturn("true");
    assertThat(ScanReportsFacetBuilder.from(scanReport).getFilterDescription()).isEmpty();
  }

  @Test
  public void testScanMetricsFullOfNulls() {
    assertThat(ScanReportsFacetBuilder.from(scanReport))
        .extracting(
            "snapshotId", "filterDescription", "projectedFieldNames", "scanMetrics", "metadata")
        .containsExactly(
            1L,
            filter.toString(),
            new String[] {"field1", "field2"},
            IcebergScanMetrics.builder().build(),
            Collections.emptyMap());
  }

  @Test
  public void testBuild() {
    when(scanMetrics.totalPlanningDuration())
        .thenReturn(TimerResult.of(TimeUnit.SECONDS, Duration.ofSeconds(1L), 1));
    when(scanMetrics.resultDataFiles()).thenReturn(CounterResult.of(Unit.COUNT, 2L));
    when(scanMetrics.resultDeleteFiles()).thenReturn(CounterResult.of(Unit.COUNT, 3L));
    when(scanMetrics.totalDataManifests()).thenReturn(CounterResult.of(Unit.COUNT, 1L));
    when(scanMetrics.totalDeleteManifests()).thenReturn(CounterResult.of(Unit.COUNT, 2L));
    when(scanMetrics.scannedDataManifests()).thenReturn(CounterResult.of(Unit.COUNT, 3L));
    when(scanMetrics.skippedDataManifests()).thenReturn(CounterResult.of(Unit.COUNT, 4L));
    when(scanMetrics.totalFileSizeInBytes()).thenReturn(CounterResult.of(Unit.BYTES, 5L));
    when(scanMetrics.totalDeleteFileSizeInBytes()).thenReturn(CounterResult.of(Unit.BYTES, 6L));
    when(scanMetrics.scannedDeleteManifests()).thenReturn(CounterResult.of(Unit.COUNT, 7L));
    when(scanMetrics.skippedDeleteManifests()).thenReturn(CounterResult.of(Unit.COUNT, 8L));
    when(scanMetrics.indexedDeleteFiles()).thenReturn(CounterResult.of(Unit.COUNT, 9L));
    when(scanMetrics.equalityDeleteFiles()).thenReturn(CounterResult.of(Unit.COUNT, 10L));
    when(scanMetrics.positionalDeleteFiles()).thenReturn(CounterResult.of(Unit.COUNT, 11L));

    assertThat(
            ScanReportsFacetBuilder.from(scanReport)
                .getScanMetrics()
                .getTotalPlanningDuration()
                .intValue())
        .isEqualTo(1000);

    assertThat(ScanReportsFacetBuilder.from(scanReport).getScanMetrics())
        .extracting(
            "resultDataFiles",
            "resultDeleteFiles",
            "totalDataManifests",
            "totalDeleteManifests",
            "scannedDataManifests",
            "skippedDataManifests",
            "totalFileSizeInBytes",
            "totalDeleteFileSizeInBytes",
            "scannedDeleteManifests",
            "skippedDeleteManifests",
            "indexedDeleteFiles",
            "equalityDeleteFiles",
            "positionalDeleteFiles")
        .containsExactly(2L, 3L, 1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L, 11L);
  }
}
