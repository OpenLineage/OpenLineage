/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.vendor.iceberg.metrics;

import com.google.common.annotations.VisibleForTesting;
import io.openlineage.spark.agent.facets.IcebergCommitReportOutputDatasetFacet;
import io.openlineage.spark.agent.facets.IcebergScanReportInputDatasetFacet;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.metrics.MetricsReporter;

/**
 * Class to store static map of Catalogs onto MetricsReporters. This is necessary as Spark session
 * can have multiple catalogs and OpenLineage may want to have a separate reporter for each of the
 * catalogs, to allow delegating reporter reporters to initially configured reporters.
 */
@Slf4j
@ToString
@EqualsAndHashCode
public class CatalogMetricsReporterHolder {

  private final Map<String, OpenLineageMetricsReporter> catalogMetricsReporter = new HashMap<>();
  ;

  private CatalogMetricsReporterHolder() {}

  public void register(Catalog catalog) {
    if (catalogMetricsReporter.containsKey(catalog.name())) {
      return;
    }
    log.debug("Registering catalog: {}", catalog);

    Field metricsReporterField = FieldUtils.getField(catalog.getClass(), "metricsReporter", true);

    if (metricsReporterField == null) {
      log.warn("Could not inject metrics reporter: no such field");
      return;
    }

    MetricsReporter existing;
    try {
      existing = (MetricsReporter) metricsReporterField.get(catalog);

      OpenLineageMetricsReporter openLineageMetricsReporter;
      if (existing instanceof OpenLineageMetricsReporter) {
        // in case the metrics reporter is manually set to OpenLineageMetricsReporter
        catalogMetricsReporter.put(catalog.name(), (OpenLineageMetricsReporter) existing);
        log.debug(
            "Existing OpenLineageMetricsReporter found, replacing metrics reporter map with: {}",
            existing);
        return;
      }

      if (existing != null) {
        log.debug("Existing metrics reporter found: {}", existing.getClass().getName());
        openLineageMetricsReporter = new OpenLineageMetricsReporter(existing);
      } else {
        log.debug("No existing metrics reporter found");
        openLineageMetricsReporter = new OpenLineageMetricsReporter();
      }

      // set metrics reporter in context
      catalogMetricsReporter.put(catalog.name(), openLineageMetricsReporter);

      // set metrics reporter in catalog
      metricsReporterField.set(catalog, openLineageMetricsReporter);
      log.info("Injected metrics reporter into Iceberg catalog");
    } catch (IllegalAccessException e) {
      log.warn("Unable to inject metrics reporter", e);
    }
  }

  /**
   * Get the commit report for the given snapshot id. If the report is found, it is removed from the
   * reporter.
   *
   * @param snapshotId snapshot id
   */
  public Optional<IcebergScanReportInputDatasetFacet> getScanReportFacet(long snapshotId) {
    Optional<IcebergScanReportInputDatasetFacet> scanReportInputDatasetFacet =
        catalogMetricsReporter.values().stream()
            .flatMap(reporter -> reporter.getScanReportFacets().stream())
            .filter(report -> report.getSnapshotId() == snapshotId)
            .findFirst();

    if (scanReportInputDatasetFacet.isPresent()) {
      log.debug("Returning scan report facet for snapshot id: {}", snapshotId);
    } else {
      log.debug("No scan report facet found for snapshot id: {}", snapshotId);
    }

    return scanReportInputDatasetFacet;
  }

  /**
   * Get the commit report for the given snapshot id. If the report is found, it is removed from the
   * reporter.
   *
   * @param snapshotId snapshot id
   */
  public Optional<IcebergCommitReportOutputDatasetFacet> getCommitReportFacet(long snapshotId) {
    Optional<IcebergCommitReportOutputDatasetFacet> commitReportOutputDatasetFacet =
        catalogMetricsReporter.values().stream()
            .flatMap(reporter -> reporter.getCommitReportFacets().stream())
            .filter(facet -> facet.getSnapshotId() == snapshotId)
            .findFirst();

    if (log.isDebugEnabled()) {
      if (catalogMetricsReporter.isEmpty()) {
        log.debug("Catalog metrics reporter is empty");
      }

      if (commitReportOutputDatasetFacet.isPresent()) {
        log.debug("Returning commit report facet for snapshot id: {}", snapshotId);
      } else {
        log.debug("No commit report facet found for snapshot id: {}", snapshotId);
      }
    }

    return commitReportOutputDatasetFacet;
  }

  @VisibleForTesting
  OpenLineageMetricsReporter getReporterFor(String catalogName) {
    return catalogMetricsReporter.get(catalogName);
  }

  @VisibleForTesting
  void clear() {
    catalogMetricsReporter.clear();
  }

  private static class SingletonHolder {
    public static final CatalogMetricsReporterHolder instance = new CatalogMetricsReporterHolder();
  }

  public static CatalogMetricsReporterHolder getInstance() {
    return CatalogMetricsReporterHolder.SingletonHolder.instance;
  }
}
