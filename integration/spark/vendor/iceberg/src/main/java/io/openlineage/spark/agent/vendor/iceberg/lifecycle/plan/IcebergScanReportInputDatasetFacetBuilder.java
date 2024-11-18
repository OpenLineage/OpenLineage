/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.vendor.iceberg.lifecycle.plan;

import io.openlineage.client.OpenLineage.InputDatasetFacet;
import io.openlineage.spark.agent.vendor.iceberg.metrics.CatalogMetricsReporterHolder;
import io.openlineage.spark.api.CustomFacetBuilder;
import io.openlineage.spark.api.OpenLineageContext;
import java.lang.reflect.InvocationTargetException;
import java.util.function.BiConsumer;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.reflect.MethodUtils;
import org.apache.iceberg.Table;
import org.apache.spark.sql.connector.read.Scan;

@Slf4j
public class IcebergScanReportInputDatasetFacetBuilder
    extends CustomFacetBuilder<Scan, InputDatasetFacet> {

  public IcebergScanReportInputDatasetFacetBuilder(OpenLineageContext context) {
    super();
  }

  @Override
  public boolean isDefinedAt(Object x) {
    if (!(x instanceof Scan)) {
      return false;
    }

    // should be defined for `org.apache.iceberg.spark.source.SparkBatchQueryScan`
    // which is not public class
    return x.getClass().getCanonicalName().startsWith("org.apache.iceberg.spark");
  }

  @Override
  protected void build(Scan x, BiConsumer<String, ? super InputDatasetFacet> consumer) {
    try {
      Table table = (Table) MethodUtils.invokeMethod(x, true, "table");

      if (table.currentSnapshot() == null) {
        return;
      }

      long snapshotId = table.currentSnapshot().snapshotId();
      CatalogMetricsReporterHolder.getInstance()
          .getScanReportFacet(snapshotId)
          .ifPresent(f -> consumer.accept("icebergScanReport", f));
    } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
      // something got wrong
      log.warn("Could not extract Iceberg scan report", e);
    }
  }
}
