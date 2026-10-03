/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.vendor.iceberg.metrics.wrapper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import io.openlineage.spark.agent.vendor.iceberg.metrics.OpenLineageMetricsReporter;
import lombok.SneakyThrows;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.metrics.CommitReport;
import org.apache.iceberg.metrics.MetricsReporter;
import org.junit.jupiter.api.Test;

class BaseTableWrapperTest {

  private final TableOperations operations = mock(TableOperations.class);
  private final MetricsReporter existing = mock(MetricsReporter.class);
  private final OpenLineageMetricsReporter reporter = new OpenLineageMetricsReporter(existing);

  @Test
  void testAttachCombinesReporterWithExistingReporter() {
    BaseTable table = new BaseTable(operations, "table", existing);

    assertThat(BaseTableWrapper.attach(table, reporter)).isTrue();

    CommitReport commitReport = mock(CommitReport.class);
    reporterOf(table).report(commitReport);

    // the reporter the table was created with is called once, not again through the delegate
    verify(existing, times(1)).report(commitReport);
    assertThat(reporter.getCommitReportFacets()).hasSize(1);
  }

  @Test
  void testAttachIsIdempotent() {
    BaseTable table = new BaseTable(operations, "table", existing);

    assertThat(BaseTableWrapper.attach(table, reporter)).isTrue();
    MetricsReporter attached = reporterOf(table);
    assertThat(BaseTableWrapper.attach(table, reporter)).isTrue();

    assertThat(reporterOf(table)).isSameAs(attached);
  }

  @Test
  void testAttachKeepsTableAlreadyReportingToOpenLineage() {
    BaseTable table = new BaseTable(operations, "table", reporter);

    assertThat(BaseTableWrapper.attach(table, reporter)).isTrue();

    assertThat(reporterOf(table)).isSameAs(reporter);
  }

  @Test
  void testAttachUsesCombineMetricsReporterWhenAvailable() {
    TableWithCombineMetricsReporter table = new TableWithCombineMetricsReporter(operations);

    assertThat(BaseTableWrapper.attach(table, reporter)).isTrue();

    assertThat(table.combined).isSameAs(reporter.getTableReporter());
    assertThat(reporterOf(table)).isSameAs(existing);
  }

  @Test
  void testAttachSkipsOtherTables() {
    Table table = mock(Table.class);

    assertThat(BaseTableWrapper.attach(table, reporter)).isFalse();
    verify(existing, never()).report(any());
  }

  @SneakyThrows
  private static MetricsReporter reporterOf(BaseTable table) {
    return (MetricsReporter) FieldUtils.readField(table, BaseTableWrapper.REPORTER, true);
  }

  /** Mimics {@code BaseTable} of Iceberg 1.11, which can combine reporters. */
  public class TableWithCombineMetricsReporter extends BaseTable {
    MetricsReporter combined;

    TableWithCombineMetricsReporter(TableOperations operations) {
      super(operations, "table", existing);
    }

    public void combineMetricsReporter(MetricsReporter metricsReporter) {
      combined = metricsReporter;
    }
  }
}
