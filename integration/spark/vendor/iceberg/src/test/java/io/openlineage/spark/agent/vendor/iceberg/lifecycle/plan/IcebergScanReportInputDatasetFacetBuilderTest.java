/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.vendor.iceberg.lifecycle.plan;

import static io.openlineage.spark.agent.vendor.iceberg.metrics.CatalogMetricsReporterHolder.VENDOR_CONTEXT_KEY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import io.openlineage.client.OpenLineage.InputDatasetFacet;
import io.openlineage.spark.agent.facets.IcebergScanReportInputDatasetFacet;
import io.openlineage.spark.agent.vendor.iceberg.metrics.CatalogMetricsReporterHolder;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.SparkOpenLineageConfig;
import io.openlineage.spark.api.Vendors;
import io.openlineage.spark.api.VendorsContext;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.apache.iceberg.metrics.ScanReport;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

class IcebergScanReportInputDatasetFacetBuilderTest {

  @Test
  void emitsScanLocalReportWithoutCatalogReporter() {
    ScanReport report = scanReport(42L);
    Map<String, InputDatasetFacet> facets = new HashMap<>();

    builder(null).build(new TestingScanWithReport(() -> report), facets::put);

    assertThat(facets.get("icebergScanReport"))
        .isInstanceOf(IcebergScanReportInputDatasetFacet.class)
        .extracting(facet -> ((IcebergScanReportInputDatasetFacet) facet).getSnapshotId())
        .isEqualTo(42L);
  }

  @Test
  void scanLocalReportTakesPrecedenceOverCatalogReporter() {
    CatalogMetricsReporterHolder holder = mock(CatalogMetricsReporterHolder.class);
    Map<String, InputDatasetFacet> facets = new HashMap<>();

    builder(holder).build(new TestingScanWithReport(() -> scanReport(42L)), facets::put);

    assertThat(facets).containsKey("icebergScanReport");
    verifyNoInteractions(holder);
  }

  @Test
  void emitsScanLocalReportOnlyOnce() {
    IcebergScanReportInputDatasetFacetBuilder builder = builder(null);
    TestingScanWithReport scan = new TestingScanWithReport(() -> scanReport(42L));
    AtomicInteger emittedFacets = new AtomicInteger();

    builder.build(scan, (name, facet) -> emittedFacets.incrementAndGet());
    builder.build(scan, (name, facet) -> emittedFacets.incrementAndGet());

    assertThat(emittedFacets).hasValue(1);
  }

  @Test
  void isNotDefinedWhenMetricsReporterIsDisabled() {
    IcebergScanReportInputDatasetFacetBuilder builder = builder(null, true);
    Map<String, InputDatasetFacet> facets = new HashMap<>();
    Scan scan = new org.apache.iceberg.spark.TestingScanWithReport(() -> scanReport(42L));

    builder.accept(scan, facets::put);

    assertThat(builder.isDefinedAt(scan)).isFalse();
    assertThat(facets).isEmpty();
  }

  @Test
  void doesNotLookUpCatalogReporterWhenScanLocalReportIsAbsentWithoutLegacyMethods() {
    CatalogMetricsReporterHolder holder = mock(CatalogMetricsReporterHolder.class);
    Map<String, InputDatasetFacet> facets = new HashMap<>();

    builder(holder).build(new TestingScanWithoutReport(), facets::put);

    assertThat(facets).isEmpty();
    verifyNoInteractions(holder);
  }

  private IcebergScanReportInputDatasetFacetBuilder builder(CatalogMetricsReporterHolder holder) {
    return builder(holder, false);
  }

  private IcebergScanReportInputDatasetFacetBuilder builder(
      CatalogMetricsReporterHolder holder, boolean metricsReporterDisabled) {
    VendorsContext vendorsContext = new VendorsContext();
    if (holder != null) {
      vendorsContext.register(VENDOR_CONTEXT_KEY, holder);
    }

    Vendors vendors = mock(Vendors.class);
    when(vendors.getVendorsContext()).thenReturn(vendorsContext);

    OpenLineageContext context = mock(OpenLineageContext.class);
    when(context.getVendors()).thenReturn(vendors);
    if (metricsReporterDisabled) {
      SparkOpenLineageConfig config = mock(SparkOpenLineageConfig.class);
      SparkOpenLineageConfig.VendorsConfig vendorsConfig =
          new SparkOpenLineageConfig.VendorsConfig();
      vendorsConfig
          .getConfig()
          .put("iceberg", new SparkOpenLineageConfig.VendorsConfig.VendorConfig(true));
      when(config.getVendors()).thenReturn(vendorsConfig);
      when(context.getOpenLineageConfig()).thenReturn(config);
    }
    return new IcebergScanReportInputDatasetFacetBuilder(context);
  }

  private ScanReport scanReport(long snapshotId) {
    ScanReport report = mock(ScanReport.class);
    when(report.snapshotId()).thenReturn(snapshotId);
    when(report.projectedFieldNames()).thenReturn(Collections.emptyList());
    when(report.metadata()).thenReturn(Collections.emptyMap());
    return report;
  }

  private abstract static class TestingScanWithReportBase implements Scan {
    private final Supplier<ScanReport> scanReportSupplier;

    private TestingScanWithReportBase(Supplier<ScanReport> scanReportSupplier) {
      this.scanReportSupplier = scanReportSupplier;
    }

    @Override
    public StructType readSchema() {
      return new StructType();
    }
  }

  private static class TestingScanWithReport extends TestingScanWithReportBase {
    private TestingScanWithReport(Supplier<ScanReport> scanReportSupplier) {
      super(scanReportSupplier);
    }
  }

  private static class TestingScanWithoutReport implements Scan {
    @Override
    public StructType readSchema() {
      return new StructType();
    }
  }
}
