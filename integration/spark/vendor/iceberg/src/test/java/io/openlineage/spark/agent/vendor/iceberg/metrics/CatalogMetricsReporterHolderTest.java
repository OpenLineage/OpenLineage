/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.vendor.iceberg.metrics;

import static io.openlineage.spark.agent.vendor.iceberg.metrics.CatalogMetricsReporterHolder.VENDOR_CONTEXT_KEY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.VendorsContext;
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

  public static final String CATALOG_NAME = "catalog-name";
  TestingIcebergCatalog icebergCatalog = new TestingIcebergCatalog();
  OpenLineageContext context = mock(OpenLineageContext.class, RETURNS_DEEP_STUBS);

  @BeforeEach
  public void setup() {
    when(context.getVendors().getVendorsContext()).thenReturn(new VendorsContext());
  }

  @Test
  void testRegister() {
    CatalogMetricsReporterHolder.register(context, icebergCatalog);

    assertThat(icebergCatalog.metricsReporter)
        .isNotNull()
        .isInstanceOf(OpenLineageMetricsReporter.class);

    assertThat(getMetricHolder().getReporterFor(CATALOG_NAME))
        .isEqualTo(icebergCatalog.metricsReporter);
  }

  @Test
  void testRegisterRunsOnce() {
    TestingIcebergCatalog anotherCatalog = new TestingIcebergCatalog();

    CatalogMetricsReporterHolder.register(context, icebergCatalog);
    CatalogMetricsReporterHolder.register(context, anotherCatalog);

    assertThat(anotherCatalog.metricsReporter).isNull();
  }

  @Test
  void testRegisterWithExistingReporter() {
    MetricsReporter existingReporter = mock(MetricsReporter.class);
    icebergCatalog.metricsReporter = existingReporter;

    CatalogMetricsReporterHolder.register(context, icebergCatalog);

    assertThat(getMetricHolder().getReporterFor(CATALOG_NAME).getDelegate())
        .isEqualTo(existingReporter);
  }

  @Test
  void testRegisterDoesNotOverwriteOpenLineageMetricsReporter() {
    MetricsReporter existingReporter = mock(OpenLineageMetricsReporter.class);
    icebergCatalog.metricsReporter = existingReporter;

    CatalogMetricsReporterHolder.register(context, icebergCatalog);

    assertThat(getMetricHolder().getReporterFor(CATALOG_NAME)).isEqualTo(existingReporter);
  }

  @Test
  void testGetScanReport() {
    ScanReport scanReport = mock(ScanReport.class);
    when(scanReport.snapshotId()).thenReturn(1L);

    CatalogMetricsReporterHolder.register(context, icebergCatalog);
    icebergCatalog.metricsReporter = getMetricHolder().getReporterFor(CATALOG_NAME);

    icebergCatalog.metricsReporter.report(scanReport);

    assertThat(getMetricHolder().getScanReportFacet(1L))
        .isPresent()
        .get()
        .extracting("snapshotId")
        .isEqualTo(1L);
  }

  @Test
  void testGetCommitReport() {
    CommitReport commitReport = mock(CommitReport.class);
    when(commitReport.snapshotId()).thenReturn(1L);

    CatalogMetricsReporterHolder.register(context, icebergCatalog);
    icebergCatalog.metricsReporter = getMetricHolder().getReporterFor(CATALOG_NAME);

    icebergCatalog.metricsReporter.report(commitReport);

    assertThat(getMetricHolder().getCommitReportFacet(1L))
        .isPresent()
        .get()
        .extracting("snapshotId")
        .isEqualTo(1L);
  }

  private CatalogMetricsReporterHolder getMetricHolder() {
    return ((CatalogMetricsReporterHolder)
        context.getVendors().getVendorsContext().fromVendorsContext(VENDOR_CONTEXT_KEY).get());
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
      return CATALOG_NAME;
    }
  }
}
