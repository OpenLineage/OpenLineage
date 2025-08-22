/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan;

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
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.command.RefreshTableCommand;

public class RefreshTableCommandVisitor
    extends QueryPlanVisitor<RefreshTableCommand, OpenLineage.InputDataset> {

  public RefreshTableCommandVisitor(OpenLineageContext context) {
    super(context);
  }

  @Override
  public List<OpenLineage.InputDataset> apply(LogicalPlan x) {
    Optional<CatalogTable> tableOption = catalogTableFor(((RefreshTableCommand) x).tableIdent());

    if (!tableOption.isPresent()) {
      return Collections.emptyList();
    }

    CatalogTable catalogTable = tableOption.get();

    DatasetIdentifier di =
        PathUtils.fromCatalogTable(catalogTable, context.getSparkSession().get());

    OpenLineage.LifecycleStateChangeDatasetFacet lifecycleStateChangeDatasetFacet =
        context
            .getOpenLineage()
            .newLifecycleStateChangeDatasetFacetBuilder()
            .lifecycleStateChange(
                OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange.ALTER)
            .build();

    DatasetFactory<OpenLineage.InputDataset> factory = inputDataset();

    DatasetCompositeFacetsBuilder datasetFacetsBuilder = factory.createCompositeFacetBuilder();
    datasetFacetsBuilder
        .getFacets()
        .schema(PlanUtils.schemaFacet(context.getOpenLineage(), catalogTable.schema()))
        .dataSource(PlanUtils.datasourceFacet(context.getOpenLineage(), di.getNamespace()))
        .lifecycleStateChange(lifecycleStateChangeDatasetFacet);

    return Collections.singletonList(factory.getDataset(di, datasetFacetsBuilder));
  }
}
