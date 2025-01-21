/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark2.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.dataset.DatasetCompositeFacetsBuilder;
import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.spark.agent.util.PathUtils;
import io.openlineage.spark.agent.util.PlanUtils;
import io.openlineage.spark.api.DatasetFactory;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.QueryPlanVisitor;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.command.AlterTableRecoverPartitionsCommand;

@Slf4j
public class AlterTableRecoverPartitionsCommandVisitor
    extends QueryPlanVisitor<AlterTableRecoverPartitionsCommand, OpenLineage.OutputDataset> {

  public AlterTableRecoverPartitionsCommandVisitor(OpenLineageContext context) {
    super(context);
  }

  @SneakyThrows
  @Override
  public List<OpenLineage.OutputDataset> apply(LogicalPlan x) {
    Optional<CatalogTable> tableOpt =
        catalogTableFor(((AlterTableRecoverPartitionsCommand) x).tableName());

    if (!tableOpt.isPresent()) {
      return Collections.emptyList();
    }
    CatalogTable table = tableOpt.get();

    DatasetIdentifier di = PathUtils.fromCatalogTable(table, context.getSparkSession().get());

    OpenLineage.LifecycleStateChangeDatasetFacet lifecycleStateChangeDatasetFacet =
        context
            .getOpenLineage()
            .newLifecycleStateChangeDatasetFacetBuilder()
            .lifecycleStateChange(
                OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange.ALTER)
            .build();

    DatasetFactory<OpenLineage.OutputDataset> factory = outputDataset();

    DatasetCompositeFacetsBuilder datasetFacetsBuilder = factory.createCompositeFacetBuilder();
    datasetFacetsBuilder
        .getFacets()
        .schema(PlanUtils.schemaFacet(context.getOpenLineage(), table.schema()))
        .dataSource(PlanUtils.datasourceFacet(context.getOpenLineage(), di.getNamespace()))
        .lifecycleStateChange(lifecycleStateChangeDatasetFacet);

    return Collections.singletonList(factory.getDataset(di, datasetFacetsBuilder));
  }
}
