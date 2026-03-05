/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.dataset.DatasetCompositeFacetsBuilder;
import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.spark.agent.util.HierarchyDatasetFacetUtils;
import io.openlineage.spark.agent.util.PathUtils;
import io.openlineage.spark.agent.util.PlanUtils;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.QueryPlanVisitor;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.scheduler.SparkListenerJobEnd;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.command.CreateTableCommand;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd;

/**
 * {@link LogicalPlan} visitor that matches an {@link CreateTableCommand} and extracts the output
 * {@link OpenLineage.Dataset} being written.
 */
@Slf4j
public class CreateTableCommandVisitor
    extends QueryPlanVisitor<CreateTableCommand, OpenLineage.OutputDataset> {

  public CreateTableCommandVisitor(OpenLineageContext context) {
    super(context);
  }

  @Override
  public boolean isDefinedAt(SparkListenerEvent event) {
    return (event instanceof SparkListenerSQLExecutionEnd || event instanceof SparkListenerJobEnd);
  }

  @Override
  public List<OpenLineage.OutputDataset> apply(LogicalPlan x) {
    if (!context.getSparkSession().isPresent()) {
      return Collections.emptyList();
    }

    CreateTableCommand command = (CreateTableCommand) x;
    CatalogTable catalogTable = command.table();
    DatasetIdentifier di =
        PathUtils.fromCatalogTable(catalogTable, context.getSparkSession().get());

    DatasetCompositeFacetsBuilder builder = outputDataset().createCompositeFacetBuilder();
    builder
        .getFacets()
        .schema(PlanUtils.schemaFacet(context.getOpenLineage(), catalogTable.schema()))
        .dataSource(PlanUtils.datasourceFacet(context.getOpenLineage(), di.getNamespace()))
        .lifecycleStateChange(
            context
                .getOpenLineage()
                .newLifecycleStateChangeDatasetFacet(
                    OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange.CREATE, null))
        .hierarchy(
            HierarchyDatasetFacetUtils.buildHierarchyFacet(
                context.getOpenLineage(), catalogTable.identifier()));

    return Collections.singletonList(outputDataset().getDataset(di, builder));
  }

  @Override
  public Optional<String> jobNameSuffix(CreateTableCommand command) {
    return context
        .getSparkSession()
        .map(session -> PathUtils.fromCatalogTable(command.table(), session))
        .map(table -> trimPath(context, table.getName()));
  }
}
