/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.vendor.iceberg.metrics;

import io.openlineage.spark.agent.facets.IcebergCommitMetrics;
import io.openlineage.spark.agent.facets.IcebergCommitReportOutputDatasetFacet;
import java.util.Collections;
import java.util.Optional;
import org.apache.iceberg.metrics.CommitMetricsResult;
import org.apache.iceberg.metrics.CommitReport;

/** This class is responsible for building the CommitReportFacet from CommitReports. */
public class CommitReportsFacetBuilder {

  public static IcebergCommitReportOutputDatasetFacet from(CommitReport commitReport) {
    return new IcebergCommitReportOutputDatasetFacet(
        commitReport.snapshotId(),
        commitReport.sequenceNumber(),
        commitReport.operation(),
        getIcebergCommitMetrics(commitReport.commitMetrics()),
        commitReport.metadata(),
        Collections.emptyMap());
  }

  private static IcebergCommitMetrics getIcebergCommitMetrics(CommitMetricsResult metricsResult) {
    IcebergCommitMetrics.IcebergCommitMetricsBuilder builder = IcebergCommitMetrics.builder();

    if (metricsResult == null) {
      return builder.build();
    }

    Optional.ofNullable(metricsResult.totalDuration())
        .ifPresent(
            totalDuration -> builder.totalDuration(totalDuration.totalDuration().toMillis()));

    Optional.ofNullable(metricsResult.attempts())
        .ifPresent(attempts -> builder.attempts(attempts.value()));

    Optional.ofNullable(metricsResult.addedDataFiles())
        .ifPresent(addedDataFiles -> builder.addedDataFiles(addedDataFiles.value()));

    Optional.ofNullable(metricsResult.removedDataFiles())
        .ifPresent(removedDataFiles -> builder.removedDataFiles(removedDataFiles.value()));

    Optional.ofNullable(metricsResult.totalDataFiles())
        .ifPresent(totalDataFiles -> builder.totalDataFiles(totalDataFiles.value()));

    Optional.ofNullable(metricsResult.addedDeleteFiles())
        .ifPresent(addedDeleteFiles -> builder.addedDeleteFiles(addedDeleteFiles.value()));

    Optional.ofNullable(metricsResult.addedEqualityDeleteFiles())
        .ifPresent(
            addedEqualityDeleteFiles ->
                builder.addedEqualityDeleteFiles(addedEqualityDeleteFiles.value()));

    Optional.ofNullable(metricsResult.addedPositionalDeleteFiles())
        .ifPresent(
            addedPositionalDeleteFiles ->
                builder.addedPositionalDeleteFiles(addedPositionalDeleteFiles.value()));

    Optional.ofNullable(metricsResult.removedDeleteFiles())
        .ifPresent(removedDeleteFiles -> builder.removedDeleteFiles(removedDeleteFiles.value()));

    Optional.ofNullable(metricsResult.removedEqualityDeleteFiles())
        .ifPresent(
            removedEqualityDeleteFiles ->
                builder.removedEqualityDeleteFiles(removedEqualityDeleteFiles.value()));

    Optional.ofNullable(metricsResult.removedPositionalDeleteFiles())
        .ifPresent(
            removedPositionalDeleteFiles ->
                builder.removedPositionalDeleteFiles(removedPositionalDeleteFiles.value()));

    Optional.ofNullable(metricsResult.totalDeleteFiles())
        .ifPresent(totalDeleteFiles -> builder.totalDeleteFiles(totalDeleteFiles.value()));

    Optional.ofNullable(metricsResult.addedRecords())
        .ifPresent(addedRecords -> builder.addedRecords(addedRecords.value()));

    Optional.ofNullable(metricsResult.removedRecords())
        .ifPresent(removedRecords -> builder.removedRecords(removedRecords.value()));

    Optional.ofNullable(metricsResult.totalRecords())
        .ifPresent(totalRecords -> builder.totalRecords(totalRecords.value()));

    Optional.ofNullable(metricsResult.addedFilesSizeInBytes())
        .ifPresent(
            addedFilesSizeInBytes -> builder.addedFilesSizeInBytes(addedFilesSizeInBytes.value()));

    Optional.ofNullable(metricsResult.removedFilesSizeInBytes())
        .ifPresent(
            removedFilesSizeInBytes ->
                builder.removedFilesSizeInBytes(removedFilesSizeInBytes.value()));

    Optional.ofNullable(metricsResult.totalFilesSizeInBytes())
        .ifPresent(
            totalFilesSizeInBytes -> builder.totalFilesSizeInBytes(totalFilesSizeInBytes.value()));

    Optional.ofNullable(metricsResult.addedEqualityDeletes())
        .ifPresent(
            addedEqualityDeletes -> builder.addedEqualityDeletes(addedEqualityDeletes.value()));

    Optional.ofNullable(metricsResult.addedPositionalDeletes())
        .ifPresent(
            addedPositionalDeletes ->
                builder.addedPositionalDeletes(addedPositionalDeletes.value()));

    Optional.ofNullable(metricsResult.removedPositionalDeletes())
        .ifPresent(
            removedPositionalDeletes ->
                builder.removedPositionalDeletes(removedPositionalDeletes.value()));

    Optional.ofNullable(metricsResult.totalPositionalDeletes())
        .ifPresent(
            totalPositionalDeletes ->
                builder.totalPositionalDeletes(totalPositionalDeletes.value()));

    Optional.ofNullable(metricsResult.addedEqualityDeletes())
        .ifPresent(
            addedEqualityDeletes -> builder.addedEqualityDeletes(addedEqualityDeletes.value()));

    Optional.ofNullable(metricsResult.removedEqualityDeletes())
        .ifPresent(
            removedEqualityDeletes ->
                builder.removedEqualityDeletes(removedEqualityDeletes.value()));

    Optional.ofNullable(metricsResult.totalEqualityDeletes())
        .ifPresent(
            totalEqualityDeletes -> builder.totalEqualityDeletes(totalEqualityDeletes.value()));

    return builder.build();
  }
}
