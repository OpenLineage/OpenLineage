/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.vendor.iceberg.metrics;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.metrics.CommitReport;
import org.apache.iceberg.metrics.MetricsReporter;
import org.apache.iceberg.metrics.ScanReport;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class CatalogMetricsReporterHolderTest {

  TestingIcebergCatalog icebergCatalog = new TestingIcebergCatalog();

  @BeforeEach
  public void setup() {
    CatalogMetricsReporterHolder.getInstance().clear();
  }

  @Test
  void testRegister() {
    CatalogMetricsReporterHolder.getInstance().register(icebergCatalog);

    assertThat(icebergCatalog.metricsReporter)
        .isNotNull()
        .isInstanceOf(OpenLineageMetricsReporter.class);

    assertThat(CatalogMetricsReporterHolder.getInstance().getReporterFor("catalog-name"))
        .isEqualTo(icebergCatalog.metricsReporter);
  }

  @Test
  void testRegisterRunsOnce() {
    TestingIcebergCatalog anotherCatalog = new TestingIcebergCatalog();

    CatalogMetricsReporterHolder.getInstance().register(icebergCatalog);
    CatalogMetricsReporterHolder.getInstance().register(anotherCatalog);

    assertThat(anotherCatalog.metricsReporter).isNull();
  }

  @Test
  void testRegisterWithExistingReporter() {
    MetricsReporter existingReporter = mock(MetricsReporter.class);
    icebergCatalog.metricsReporter = existingReporter;

    CatalogMetricsReporterHolder.getInstance().register(icebergCatalog);

    assertThat(
            CatalogMetricsReporterHolder.getInstance().getReporterFor("catalog-name").getDelegate())
        .isEqualTo(existingReporter);
  }

  @Test
  void testRegisterDoesNotOverwriteOpenLineageMetricsReporter() {
    MetricsReporter existingReporter = mock(OpenLineageMetricsReporter.class);
    icebergCatalog.metricsReporter = existingReporter;

    CatalogMetricsReporterHolder.getInstance().register(icebergCatalog);

    assertThat(CatalogMetricsReporterHolder.getInstance().getReporterFor("catalog-name"))
        .isEqualTo(existingReporter);
  }

  @Test
  void testGetScanReport() {
    ScanReport scanReport = mock(ScanReport.class);
    when(scanReport.snapshotId()).thenReturn(1L);

    CatalogMetricsReporterHolder.getInstance().register(icebergCatalog);
    icebergCatalog.metricsReporter =
        CatalogMetricsReporterHolder.getInstance().getReporterFor("catalog-name");

    icebergCatalog.metricsReporter.report(scanReport);

    assertThat(CatalogMetricsReporterHolder.getInstance().getScanReportFacet(1L))
        .isPresent()
        .get()
        .extracting("snapshotId")
        .isEqualTo(1L);
  }

  @Test
  void testGetCommitReport() {
    CommitReport commitReport = mock(CommitReport.class);
    when(commitReport.snapshotId()).thenReturn(1L);

    CatalogMetricsReporterHolder.getInstance().register(icebergCatalog);
    icebergCatalog.metricsReporter =
        CatalogMetricsReporterHolder.getInstance().getReporterFor("catalog-name");

    icebergCatalog.metricsReporter.report(commitReport);

    assertThat(CatalogMetricsReporterHolder.getInstance().getCommitReportFacet(1L))
        .isPresent()
        .get()
        .extracting("snapshotId")
        .isEqualTo(1L);
  }

  private static class TestingIcebergCatalog implements Catalog {

    public MetricsReporter metricsReporter;

    @Override
    public List<TableIdentifier> listTables(Namespace namespace) {
      return null;
    }

    @Override
    public boolean dropTable(TableIdentifier tableIdentifier, boolean b) {
      return false;
    }

    @Override
    public void renameTable(TableIdentifier tableIdentifier, TableIdentifier tableIdentifier1) {}

    @Override
    public Table loadTable(TableIdentifier tableIdentifier) {
      return null;
    }

    public String name() {
      return "catalog-name";
    }
  }
}
