/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.vendor.iceberg.metrics;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.Collections;
import java.util.Map;
import java.util.stream.IntStream;
import org.apache.iceberg.metrics.CommitReport;
import org.apache.iceberg.metrics.MetricsReport;
import org.apache.iceberg.metrics.MetricsReporter;
import org.apache.iceberg.metrics.ScanReport;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class OpenLineageMetricsReporterTest {

  OpenLineageMetricsReporter reporter = new OpenLineageMetricsReporter();

  @BeforeEach
  public void beforeEach() {
    reporter.initialize(Collections.emptyMap());
  }

  @Test
  void testMaxSizeNotExceeded() {
    IntStream.range(0, 100).forEach(i -> reporter.report(mock(CommitReport.class)));
    IntStream.range(0, 100).forEach(i -> reporter.report(mock(ScanReport.class)));

    assertThat(reporter.getCommitReportFacets()).hasSize(50);
    assertThat(reporter.getScanReportFacets()).hasSize(50);
  }

  @Test
  void testDelegateIsCalled() {
    MetricsReporter delegate = mock(MetricsReporter.class);
    Map<String, String> initializeMap = Collections.singletonMap("a", "b");
    MetricsReport metricsReport = mock(MetricsReport.class);

    reporter = new OpenLineageMetricsReporter(delegate);
    reporter.initialize(initializeMap);
    reporter.report(metricsReport);

    verify(delegate, times(1)).initialize(initializeMap);
    verify(delegate, times(1)).report(metricsReport);
  }
}
