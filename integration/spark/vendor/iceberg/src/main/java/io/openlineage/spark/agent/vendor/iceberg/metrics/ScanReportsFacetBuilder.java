/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.vendor.iceberg.metrics;

import io.openlineage.spark.agent.facets.IcebergScanMetrics;
import io.openlineage.spark.agent.facets.IcebergScanMetrics.IcebergScanMetricsBuilder;
import io.openlineage.spark.agent.facets.IcebergScanReportInputDatasetFacet;
import java.util.Collections;
import java.util.Objects;
import java.util.Optional;
import org.apache.iceberg.metrics.ScanMetricsResult;
import org.apache.iceberg.metrics.ScanReport;

/** This class is responsible for building the ScanReportsFacet from ScanReports. */
public class ScanReportsFacetBuilder {

  public static IcebergScanReportInputDatasetFacet from(ScanReport scanReport) {
    String filter = Objects.toString(scanReport.filter());
    return new IcebergScanReportInputDatasetFacet(
        scanReport.snapshotId(),
        "true".equalsIgnoreCase(filter) ? "" : filter,
        scanReport.projectedFieldNames().toArray(new String[0]),
        getIcebergScanMetrics(scanReport.scanMetrics()),
        scanReport.metadata(),
        Collections.emptyMap());
  }

  private static IcebergScanMetrics getIcebergScanMetrics(ScanMetricsResult metricsResult) {
    IcebergScanMetricsBuilder builder = IcebergScanMetrics.builder();

    if (metricsResult == null) {
      return builder.build();
    }

    Optional.ofNullable(metricsResult.totalPlanningDuration())
        .ifPresent(
            totalPlanningDuration ->
                builder.totalPlanningDuration(totalPlanningDuration.totalDuration().toMillis()));

    Optional.ofNullable(metricsResult.resultDataFiles())
        .ifPresent(resultDataFiles -> builder.resultDataFiles(resultDataFiles.value()));

    Optional.ofNullable(metricsResult.resultDeleteFiles())
        .ifPresent(resultDeleteFiles -> builder.resultDeleteFiles(resultDeleteFiles.value()));

    Optional.ofNullable(metricsResult.totalDataManifests())
        .ifPresent(totalDataManifests -> builder.totalDataManifests(totalDataManifests.value()));

    Optional.ofNullable(metricsResult.totalDeleteManifests())
        .ifPresent(
            totalDeleteManifests -> builder.totalDeleteManifests(totalDeleteManifests.value()));

    Optional.ofNullable(metricsResult.scannedDataManifests())
        .ifPresent(
            scannedDataManifests -> builder.scannedDataManifests(scannedDataManifests.value()));

    Optional.ofNullable(metricsResult.skippedDataManifests())
        .ifPresent(
            skippedDataManifests -> builder.skippedDataManifests(skippedDataManifests.value()));

    Optional.ofNullable(metricsResult.totalFileSizeInBytes())
        .ifPresent(
            totalFileSizeInBytes -> builder.totalFileSizeInBytes(totalFileSizeInBytes.value()));

    Optional.ofNullable(metricsResult.totalDeleteFileSizeInBytes())
        .ifPresent(
            totalDeleteFileSizeInBytes ->
                builder.totalDeleteFileSizeInBytes(totalDeleteFileSizeInBytes.value()));

    Optional.ofNullable(metricsResult.skippedDataFiles())
        .ifPresent(skippedDataFiles -> builder.skippedDataFiles(skippedDataFiles.value()));

    Optional.ofNullable(metricsResult.skippedDeleteFiles())
        .ifPresent(skippedDeleteFiles -> builder.skippedDeleteFiles(skippedDeleteFiles.value()));

    Optional.ofNullable(metricsResult.scannedDeleteManifests())
        .ifPresent(
            scannedDeleteManifests ->
                builder.scannedDeleteManifests(scannedDeleteManifests.value()));

    Optional.ofNullable(metricsResult.skippedDeleteManifests())
        .ifPresent(
            skippedDeleteManifests ->
                builder.skippedDeleteManifests(skippedDeleteManifests.value()));

    Optional.ofNullable(metricsResult.indexedDeleteFiles())
        .ifPresent(indexedDeleteFiles -> builder.indexedDeleteFiles(indexedDeleteFiles.value()));

    Optional.ofNullable(metricsResult.equalityDeleteFiles())
        .ifPresent(equalityDeleteFiles -> builder.equalityDeleteFiles(equalityDeleteFiles.value()));

    Optional.ofNullable(metricsResult.positionalDeleteFiles())
        .ifPresent(
            positionalDeleteFiles -> builder.positionalDeleteFiles(positionalDeleteFiles.value()));

    return builder.build();
  }
}
