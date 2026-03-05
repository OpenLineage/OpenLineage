/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.dataset.DatasetCompositeFacetsBuilder;
import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.spark.agent.util.CatalogDatasetFacetUtils;
import io.openlineage.spark.agent.util.HierarchyDatasetFacetUtils;
import io.openlineage.spark.agent.util.PathUtils;
import io.openlineage.spark.agent.util.PlanUtils;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.QueryPlanVisitor;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.hive.execution.InsertIntoHiveTable;

/**
 * {@link LogicalPlan} visitor that matches an {@link InsertIntoHiveTable} and extracts the output
 * {@link OpenLineage.Dataset} being written.
 */
public class InsertIntoHiveTableVisitor
    extends QueryPlanVisitor<InsertIntoHiveTable, OpenLineage.OutputDataset> {

  public InsertIntoHiveTableVisitor(OpenLineageContext context) {
    super(context);
  }

  public static boolean hasHiveClasses() {
    try {
      InsertIntoHiveTableVisitor.class
          .getClassLoader()
          .loadClass("org.apache.spark.sql.hive.execution.InsertIntoHiveTable");
      return true;
    } catch (ClassNotFoundException e) {
      // ignore
    }
    return false;
  }

  @Override
  public List<OpenLineage.OutputDataset> apply(LogicalPlan x) {
    if (!context.getSparkSession().isPresent()) {
      return Collections.emptyList();
    }

    InsertIntoHiveTable cmd = (InsertIntoHiveTable) x;
    CatalogTable table = cmd.table();
    DatasetIdentifier di = PathUtils.fromCatalogTable(table, context.getSparkSession().get());

    DatasetCompositeFacetsBuilder builder = outputDataset().createCompositeFacetBuilder();
    builder
        .getFacets()
        .schema(PlanUtils.schemaFacet(context.getOpenLineage(), table.schema()))
        .dataSource(PlanUtils.datasourceFacet(context.getOpenLineage(), di.getNamespace()))
        .hierarchy(
            HierarchyDatasetFacetUtils.buildHierarchyFacet(
                context.getOpenLineage(), table.identifier()));
    if (cmd.overwrite()) {
      builder
          .getFacets()
          .lifecycleStateChange(
              context
                  .getOpenLineage()
                  .newLifecycleStateChangeDatasetFacet(
                      OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange.OVERWRITE,
                      null));
    }
    CatalogDatasetFacetUtils.getCatalogDatasetFacetForHive(context)
        .ifPresent(catalogDatasetFacet -> builder.getFacets().catalog(catalogDatasetFacet));

    return Collections.singletonList(outputDataset().getDataset(di, builder));
  }

  @Override
  public Optional<String> jobNameSuffix(InsertIntoHiveTable plan) {
    return context
        .getSparkSession()
        .map(session -> PathUtils.fromCatalogTable(plan.table(), session))
        .map(table -> trimPath(context, table.getName()));
  }
}
