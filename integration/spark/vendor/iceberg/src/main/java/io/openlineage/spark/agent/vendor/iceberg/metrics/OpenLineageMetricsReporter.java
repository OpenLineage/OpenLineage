/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.vendor.iceberg.metrics;

import com.google.common.annotations.VisibleForTesting;
import io.openlineage.spark.agent.facets.IcebergCommitReportOutputDatasetFacet;
import io.openlineage.spark.agent.facets.IcebergScanReportInputDatasetFacet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.metrics.CommitReport;
import org.apache.iceberg.metrics.MetricsReport;
import org.apache.iceberg.metrics.MetricsReporter;
import org.apache.iceberg.metrics.ScanReport;

/**
 * OpenLineageMetricsReporter is a custom implementation of the Iceberg MetricsReporter interface.
 * It stores limited amount of metrics data in memory and routes method calls to other metric
 * reporter if it is set. Additionally, it stores OpenLineage facets to make sure not memory
 * exhausting data structures are stored. Amount of stored facets is limited by MAX_FACETS_STORED
 * constant and first in first out policy.
 */
@Slf4j
public class OpenLineageMetricsReporter implements MetricsReporter {

  private static final int MAX_FACETS_STORED = 50;

  private final Optional<MetricsReporter> delegate;

  @Getter private final List<IcebergCommitReportOutputDatasetFacet> commitReportFacets;
  @Getter private final List<IcebergScanReportInputDatasetFacet> scanReportFacets;

  public OpenLineageMetricsReporter(MetricsReporter delegate) {
    log.debug("Creating OpenLineageMetricsReporter with delegate: {}", delegate);
    this.delegate = Optional.of(delegate);
    this.commitReportFacets = new LinkedList<>();
    this.scanReportFacets = new LinkedList<>();
  }

  public OpenLineageMetricsReporter() {
    log.debug("Creating OpenLineageMetricsReporter without delegate");
    this.delegate = Optional.empty();
    this.commitReportFacets = new LinkedList<>();
    this.scanReportFacets = new LinkedList<>();
  }

  @Override
  public void initialize(Map<String, String> properties) {
    log.debug("Initializing OpenLineageMetricsReporter with properties: {}", properties);
    delegate.ifPresent(d -> d.initialize(properties));
  }

  @Override
  public void report(MetricsReport metricsReport) {
    log.debug("Reporting metrics: {} to {}", metricsReport, this);
    delegate.ifPresent(delegate -> delegate.report(metricsReport));

    if (metricsReport instanceof CommitReport) {
      commitReportFacets.add(CommitReportsFacetBuilder.from((CommitReport) metricsReport));
      log.debug("CommitReportFacet added to OpenLineageMetricsReporter {}", this);
    } else if (metricsReport instanceof ScanReport) {
      log.debug("ScanReportFacet added to OpenLineageMetricsReporter");
      scanReportFacets.add(ScanReportsFacetBuilder.from((ScanReport) metricsReport));
    }

    if (scanReportFacets.size() > MAX_FACETS_STORED) {
      log.debug("Removing oldest scan report facet");
      scanReportFacets.remove(0);
    }

    if (commitReportFacets.size() > MAX_FACETS_STORED) {
      log.debug("Removing oldest commit report facet");
      commitReportFacets.remove(0);
    }
  }

  @VisibleForTesting
  public MetricsReporter getDelegate() {
    return delegate.orElse(null);
  }
}
