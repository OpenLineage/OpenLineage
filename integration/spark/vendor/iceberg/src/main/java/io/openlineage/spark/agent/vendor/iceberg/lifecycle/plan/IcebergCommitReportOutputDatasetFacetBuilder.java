/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.vendor.iceberg.lifecycle.plan;

import static io.openlineage.spark.agent.vendor.iceberg.metrics.CatalogMetricsReporterHolder.VENDOR_CONTEXT_KEY;

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

  private final OpenLineageContext context;

  public IcebergCommitReportOutputDatasetFacetBuilder(OpenLineageContext context) {
    super();
    this.context = context;
  }

  @Override
  protected void build(
      DatasetVersionDatasetFacet datasetVersionDatasetFacet,
      BiConsumer<String, ? super OutputDatasetFacet> consumer) {
    long snapshotId = Long.parseLong(datasetVersionDatasetFacet.getDatasetVersion());

    context
        .getVendors()
        .getVendorsContext()
        .fromVendorsContext(VENDOR_CONTEXT_KEY)
        .map(CatalogMetricsReporterHolder.class::cast)
        .flatMap(c -> c.getCommitReportFacet(snapshotId))
        .ifPresent(f -> consumer.accept("icebergCommitReport", f));
  }
}
