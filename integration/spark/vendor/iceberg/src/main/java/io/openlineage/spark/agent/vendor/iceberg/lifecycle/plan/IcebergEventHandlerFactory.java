/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.vendor.iceberg.lifecycle.plan;

import io.openlineage.client.OpenLineage.InputDataset;
import io.openlineage.client.OpenLineage.InputDatasetFacet;
import io.openlineage.client.OpenLineage.OutputDatasetFacet;
import io.openlineage.spark.agent.vendor.iceberg.metrics.IcebergMetricsReporterInjector;
import io.openlineage.spark.api.CustomFacetBuilder;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.OpenLineageEventHandlerFactory;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import scala.PartialFunction;

public class IcebergEventHandlerFactory implements OpenLineageEventHandlerFactory {

  @Override
  public Collection<CustomFacetBuilder<?, ? extends InputDatasetFacet>>
      createInputDatasetFacetBuilders(OpenLineageContext context) {
    return Arrays.asList(
        new IcebergInputStatisticsInputDatasetFacetBuilder(context),
        new IcebergScanReportInputDatasetFacetBuilder(context));
  }

  @Override
  public Collection<CustomFacetBuilder<?, ? extends OutputDatasetFacet>>
      createOutputDatasetFacetBuilders(OpenLineageContext context) {
    return Arrays.asList(new IcebergCommitReportOutputDatasetFacetBuilder(context));
  }

  @Override
  public Collection<PartialFunction<LogicalPlan, List<InputDataset>>>
      createInputDatasetQueryPlanVisitors(OpenLineageContext context) {
    return Arrays.asList(new IcebergMetricsReporterInjector(context));
  }
}
