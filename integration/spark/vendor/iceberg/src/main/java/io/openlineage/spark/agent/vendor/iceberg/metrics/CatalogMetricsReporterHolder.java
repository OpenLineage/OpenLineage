/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.vendor.iceberg.metrics;

import com.google.common.annotations.VisibleForTesting;
import io.openlineage.spark.agent.facets.IcebergCommitReportOutputDatasetFacet;
import io.openlineage.spark.agent.facets.IcebergScanReportInputDatasetFacet;
import io.openlineage.spark.agent.vendor.iceberg.metrics.wrapper.BaseMetastoreCatalogWrapper;
import io.openlineage.spark.agent.vendor.iceberg.metrics.wrapper.CatalogWrapper;
import io.openlineage.spark.agent.vendor.iceberg.metrics.wrapper.RESTCatalogWrapper;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.VendorsContext;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.BaseMetastoreCatalog;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.metrics.MetricsReporter;
import org.apache.iceberg.rest.RESTCatalog;

/**
 * Class to store static map of Catalogs onto MetricsReporters. This is necessary as Spark session
 * can have multiple catalogs and OpenLineage may want to have a separate reporter for each of the
 * catalogs, to allow delegating reporter reporters to initially configured reporters.
 */
@Slf4j
@ToString
@EqualsAndHashCode
public class CatalogMetricsReporterHolder {

  public static final String VENDOR_CONTEXT_KEY = "iceberg.catalogMetricsReporterHolder";

  private final Map<String, OpenLineageMetricsReporter> catalogMetricsReporter = new HashMap<>();

  private CatalogMetricsReporterHolder() {}

  public static void register(OpenLineageContext context, Catalog catalog) {
    VendorsContext vendorsContext = context.getVendors().getVendorsContext();

    if (!vendorsContext.contains(VENDOR_CONTEXT_KEY)) {
      log.debug("Creating new catalog metrics reporter holder");
      vendorsContext.register(VENDOR_CONTEXT_KEY, new CatalogMetricsReporterHolder());
    }

    CatalogMetricsReporterHolder holder =
        vendorsContext
            .fromVendorsContext(VENDOR_CONTEXT_KEY)
            .map(CatalogMetricsReporterHolder.class::cast)
            .get();

    if (holder.catalogMetricsReporter.containsKey(catalog.name())) {
      log.debug("Catalog already registered: {}", catalog);
      return;
    }
    log.debug("Registering catalog: {}", catalog);

    // use catalog wrapper objects to inject metrics reporter into different types of catalogs
    CatalogWrapper catalogWrapper;
    if (catalog instanceof BaseMetastoreCatalog) {
      catalogWrapper = new BaseMetastoreCatalogWrapper((BaseMetastoreCatalog) catalog);
    } else if (catalog instanceof RESTCatalog) {
      catalogWrapper = new RESTCatalogWrapper((RESTCatalog) catalog);
    } else {
      log.warn("Catalog type not supported: {}", catalog.getClass().getName());
      return;
    }

    // check if the metrics reporter is already set in the catalog
    MetricsReporter existing = catalogWrapper.getExistingReporter();
    OpenLineageMetricsReporter openLineageMetricsReporter;

    // check if the existing reporter is already OpenLineageMetricsReporter
    if (existing instanceof OpenLineageMetricsReporter) {
      // in case the metrics reporter is manually set to OpenLineageMetricsReporter
      holder.catalogMetricsReporter.putIfAbsent(
          catalog.name(), (OpenLineageMetricsReporter) existing);
      log.debug(
          "Existing OpenLineageMetricsReporter found, replacing metrics reporter map with: {} for runId {}",
          existing,
          context.getRunUuid());
      return;
    }

    if (existing != null) {
      // if the metrics reporter is already set in the catalog but is other than
      // OpenLineageMetricsReporter
      log.debug("Existing metrics reporter found: {}", existing.getClass().getName());
      openLineageMetricsReporter = new OpenLineageMetricsReporter(existing);
    } else if (holder.catalogMetricsReporter.containsKey(catalog.name())) {
      // if the metrics reporter is already set in the holder -> perhaps from another run
      log.debug("Use reporter available in the holder");
      openLineageMetricsReporter = holder.catalogMetricsReporter.get(catalog.name());
    } else {
      // create new metrics reporter
      log.debug("No existing metrics reporter found");
      openLineageMetricsReporter = new OpenLineageMetricsReporter();
    }

    // set metrics reporter in context
    holder.catalogMetricsReporter.put(catalog.name(), openLineageMetricsReporter);

    try {
      // set metrics reporter field in catalog
      catalogWrapper.updateMetricsReporter(openLineageMetricsReporter);
      log.info("Injected metrics reporter into Iceberg catalog and runId {}", context.getRunUuid());
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
    Optional<IcebergScanReportInputDatasetFacet> scanReport = Optional.empty();
    for (OpenLineageMetricsReporter reporter : catalogMetricsReporter.values()) {
      List<IcebergScanReportInputDatasetFacet> scanReportFacets = reporter.getScanReportFacets();
      synchronized (scanReportFacets) {
        scanReport =
            scanReportFacets.stream()
                .filter(facet -> facet.getSnapshotId() == snapshotId)
                .findAny();
        scanReport.ifPresent(scanReportFacets::remove);
      }
    }

    if (log.isDebugEnabled()) {
      if (catalogMetricsReporter.isEmpty()) {
        log.debug("Catalog metrics reporter is empty");
      }

      if (scanReport.isPresent()) {
        log.debug("Returning scan report facet for snapshot id: {}", snapshotId);
      } else {
        log.debug("No scan report facet found for snapshot id: {}", snapshotId);
      }
    }

    return scanReport;
  }

  /**
   * Get the commit report for the given snapshot id. If the report is found, it is removed from the
   * reporter.
   *
   * @param snapshotId snapshot id
   */
  public Optional<IcebergCommitReportOutputDatasetFacet> getCommitReportFacet(long snapshotId) {
    Optional<IcebergCommitReportOutputDatasetFacet> commitReport = Optional.empty();
    for (OpenLineageMetricsReporter reporter : catalogMetricsReporter.values()) {
      List<IcebergCommitReportOutputDatasetFacet> commitReportFacets =
          reporter.getCommitReportFacets();
      synchronized (commitReportFacets) {
        commitReport =
            commitReportFacets.stream()
                .filter(facet -> facet.getSnapshotId() == snapshotId)
                .findAny();
      }
    }

    if (log.isDebugEnabled()) {
      if (catalogMetricsReporter.isEmpty()) {
        log.debug("Catalog metrics reporter is empty");
      }

      if (commitReport.isPresent()) {
        log.debug("Returning commit report facet for snapshot id: {}", snapshotId);
      } else {
        log.debug("No commit report facet found for snapshot id: {}", snapshotId);
      }
    }

    return commitReport;
  }

  @VisibleForTesting
  OpenLineageMetricsReporter getReporterFor(String catalogName) {
    return catalogMetricsReporter.get(catalogName);
  }

  @VisibleForTesting
  void clear() {
    catalogMetricsReporter.clear();
  }
}
