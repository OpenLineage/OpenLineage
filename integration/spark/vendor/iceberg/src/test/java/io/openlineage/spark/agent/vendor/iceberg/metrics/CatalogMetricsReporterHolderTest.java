/*
/* Copyright 2018-2026 contributors to the OpenLineage project
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
import java.lang.reflect.Field;
import java.util.List;
import lombok.SneakyThrows;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.iceberg.BaseMetastoreCatalog;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.metrics.CommitReport;
import org.apache.iceberg.metrics.MetricsReporter;
import org.apache.iceberg.metrics.ScanReport;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.rest.RESTSessionCatalog;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class CatalogMetricsReporterHolderTest {

  public static final String CATALOG_NAME = "catalog-name";
  TestingIcebergCatalog icebergCatalog;
  OpenLineageContext context = mock(OpenLineageContext.class, RETURNS_DEEP_STUBS);

  @BeforeEach
  public void setup() {
    when(context.getVendors().getVendorsContext()).thenReturn(new VendorsContext());
    icebergCatalog = new TestingIcebergCatalog();
  }

  @Test
  void testRegister() {
    CatalogMetricsReporterHolder.register(context, icebergCatalog);

    assertThat(getMetricsReporter(icebergCatalog))
        .isNotNull()
        .isInstanceOf(OpenLineageMetricsReporter.class);

    assertThat(getMetricHolder().getReporterFor(CATALOG_NAME))
        .isEqualTo(getMetricsReporter(icebergCatalog));
  }

  @Test
  void testRegisterRunsOnce() {
    TestingIcebergCatalog anotherCatalog = new TestingIcebergCatalog();

    CatalogMetricsReporterHolder.register(context, icebergCatalog);
    CatalogMetricsReporterHolder.register(context, anotherCatalog);

    assertThat(getMetricsReporter(anotherCatalog)).isNull();
  }

  @Test
  @SneakyThrows
  void testRegisterRestCatalog() {
    MetricsReporter reporter = mock(MetricsReporter.class);
    RESTCatalog restCatalog = new RESTCatalog();
    Field sessionCatalogField = FieldUtils.getField(RESTCatalog.class, "sessionCatalog", true);
    RESTSessionCatalog sessionCatalog = (RESTSessionCatalog) sessionCatalogField.get(restCatalog);
    FieldUtils.writeField(sessionCatalog, "reporter", reporter, true);
    FieldUtils.writeField(sessionCatalog, "name", CATALOG_NAME, true);

    CatalogMetricsReporterHolder.register(context, restCatalog);

    assertThat(getMetricHolder().getReporterFor(CATALOG_NAME))
        .isInstanceOf(OpenLineageMetricsReporter.class)
        .extracting("delegate")
        .isEqualTo(reporter);
  }

  @Test
  @SneakyThrows
  void testRegisterWithExistingReporter() {
    MetricsReporter existingReporter = mock(MetricsReporter.class);
    FieldUtils.writeField(icebergCatalog, "metricsReporter", existingReporter, true);

    CatalogMetricsReporterHolder.register(context, icebergCatalog);

    assertThat(getMetricHolder().getReporterFor(CATALOG_NAME).getDelegate())
        .isEqualTo(existingReporter);
  }

  @Test
  @SneakyThrows
  void testRegisterDoesNotOverwriteOpenLineageMetricsReporter() {
    MetricsReporter existingReporter = mock(OpenLineageMetricsReporter.class);
    FieldUtils.writeField(icebergCatalog, "metricsReporter", existingReporter, true);

    CatalogMetricsReporterHolder.register(context, icebergCatalog);

    assertThat(getMetricHolder().getReporterFor(CATALOG_NAME)).isEqualTo(existingReporter);
  }

  @Test
  void testGetScanReport() {
    ScanReport scanReport = mock(ScanReport.class);
    when(scanReport.snapshotId()).thenReturn(1L);

    CatalogMetricsReporterHolder.register(context, icebergCatalog);
    getMetricsReporter(icebergCatalog).report(scanReport);

    assertThat(getMetricHolder().getScanReportFacet(1L))
        .isPresent()
        .get()
        .extracting("snapshotId")
        .isEqualTo(1L);

    // Test that the facet is removed after being retrieved
    assertThat(getMetricHolder().getScanReportFacet(1L)).isEmpty();
  }

  @Test
  void testGetCommitReport() {
    CommitReport commitReport = mock(CommitReport.class);
    when(commitReport.snapshotId()).thenReturn(1L);

    CatalogMetricsReporterHolder.register(context, icebergCatalog);
    getMetricsReporter(icebergCatalog).report(commitReport);

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

  @SneakyThrows
  private MetricsReporter getMetricsReporter(BaseMetastoreCatalog catalog) {
    Field field = FieldUtils.getField(catalog.getClass(), "metricsReporter", true);
    return (MetricsReporter) field.get(catalog);
  }

  private static class TestingIcebergCatalog extends BaseMetastoreCatalog {

    @Override
    public String name() {
      return CATALOG_NAME;
    }

    @Override
    protected TableOperations newTableOps(TableIdentifier tableIdentifier) {
      return null;
    }

    @Override
    protected String defaultWarehouseLocation(TableIdentifier tableIdentifier) {
      return "";
    }

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
  }
}
