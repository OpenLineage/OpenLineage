/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.vendor.iceberg.lifecycle.plan;

import static io.openlineage.spark.agent.vendor.iceberg.metrics.CatalogMetricsReporterHolder.VENDOR_CONTEXT_KEY;

import io.openlineage.client.OpenLineage.InputDatasetFacet;
import io.openlineage.spark.agent.vendor.iceberg.metrics.CatalogMetricsReporterHolder;
import io.openlineage.spark.agent.vendor.iceberg.metrics.ScanReportsFacetBuilder;
import io.openlineage.spark.api.CustomFacetBuilder;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.SparkOpenLineageConfig;
import io.openlineage.spark.api.SparkOpenLineageConfig.VendorsConfig;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.commons.lang3.reflect.MethodUtils;
import org.apache.iceberg.Table;
import org.apache.iceberg.metrics.ScanReport;
import org.apache.spark.sql.connector.read.Scan;

@Slf4j
public class IcebergScanReportInputDatasetFacetBuilder
    extends CustomFacetBuilder<Scan, InputDatasetFacet> {

  private final OpenLineageContext context;
  private final Set<Scan> reportedScans =
      Collections.synchronizedSet(Collections.newSetFromMap(new IdentityHashMap<>()));

  public IcebergScanReportInputDatasetFacetBuilder(OpenLineageContext context) {
    super();
    this.context = context;
  }

  @Override
  public boolean isDefinedAt(Object x) {
    if (Optional.ofNullable(context.getOpenLineageConfig())
        .map(SparkOpenLineageConfig::getVendors)
        .map(VendorsConfig::getConfig)
        .flatMap(config -> Optional.ofNullable(config.get("iceberg")))
        .map(VendorsConfig.VendorConfig::getMetricsReporterDisabled)
        .orElse(false)) {
      return false;
    }

    if (!(x instanceof Scan)) {
      return false;
    }

    // should be defined for `org.apache.iceberg.spark.source.SparkBatchQueryScan`
    // which is not public class
    return x.getClass().getCanonicalName().startsWith("org.apache.iceberg.spark");
  }

  @Override
  protected void build(Scan x, BiConsumer<String, ? super InputDatasetFacet> consumer) {
    Optional<InputDatasetFacet> scanReport = getScanReport(x);
    if (scanReport.isPresent()) {
      if (reportedScans.add(x)) {
        consumer.accept("icebergScanReport", scanReport.get());
      }
      return;
    }

    try {
      Table table = (Table) MethodUtils.invokeMethod(x, true, "table");

      if (table.currentSnapshot() == null) {
        return;
      }

      long snapshotId = table.currentSnapshot().snapshotId();
      context
          .getVendors()
          .getVendorsContext()
          .fromVendorsContext(VENDOR_CONTEXT_KEY)
          .map(CatalogMetricsReporterHolder.class::cast)
          .flatMap(c -> c.getScanReportFacet(snapshotId))
          .ifPresent(f -> consumer.accept("icebergScanReport", f));
    } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
      // something got wrong
      log.warn("Could not extract Iceberg scan report", e);
    }
  }

  private Optional<InputDatasetFacet> getScanReport(Scan scan) {
    Field field = FieldUtils.getField(scan.getClass(), "scanReportSupplier", true);
    if (field == null) {
      return Optional.empty();
    }

    try {
      Object value = field.get(scan);
      if (!(value instanceof Supplier)) {
        return Optional.empty();
      }

      Object report = ((Supplier<?>) value).get();
      if (report instanceof ScanReport) {
        return Optional.of(ScanReportsFacetBuilder.from((ScanReport) report));
      }
    } catch (IllegalAccessException | RuntimeException e) {
      log.debug("Could not extract scan-local Iceberg report", e);
    }

    return Optional.empty();
  }
}
