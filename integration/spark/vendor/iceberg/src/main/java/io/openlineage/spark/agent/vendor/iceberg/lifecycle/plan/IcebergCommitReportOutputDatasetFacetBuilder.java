/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.vendor.iceberg.lifecycle.plan;

import io.openlineage.client.OpenLineage.DatasetVersionDatasetFacet;
import io.openlineage.client.OpenLineage.OutputDatasetFacet;
import io.openlineage.spark.agent.vendor.iceberg.metrics.CatalogMetricsReporterHolder;
import io.openlineage.spark.api.CustomFacetBuilder;
import io.openlineage.spark.api.OpenLineageContext;
import java.util.function.BiConsumer;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class IcebergCommitReportOutputDatasetFacetBuilder
    extends CustomFacetBuilder<DatasetVersionDatasetFacet, OutputDatasetFacet> {

  public IcebergCommitReportOutputDatasetFacetBuilder(OpenLineageContext context) {
    super();
  }

  @Override
  protected void build(
      DatasetVersionDatasetFacet datasetVersionDatasetFacet,
      BiConsumer<String, ? super OutputDatasetFacet> consumer) {
    long snapshotId = Long.parseLong(datasetVersionDatasetFacet.getDatasetVersion());
    CatalogMetricsReporterHolder.getInstance()
        .getCommitReportFacet(snapshotId)
        .ifPresent(f -> consumer.accept("icebergCommitReport", f));
  }
}
