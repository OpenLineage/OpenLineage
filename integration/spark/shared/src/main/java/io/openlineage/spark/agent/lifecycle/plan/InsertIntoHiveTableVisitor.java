/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.util.PathUtils;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.QueryPlanVisitor;
import io.openlineage.spark.api.SparkDatasetBuilder;
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
    SparkDatasetBuilder<OpenLineage.OutputDataset> builder =
        outputDataset().sparkDatasetBuilder().dataset(table).schema(table.schema()).catalog();
    if (cmd.overwrite()) {
      builder.lifecycleStateChange(
          OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange.OVERWRITE);
    }
    return Collections.singletonList(builder.build());
  }

  @Override
  public Optional<String> jobNameSuffix(InsertIntoHiveTable plan) {
    return context
        .getSparkSession()
        .map(session -> PathUtils.fromCatalogTable(plan.table(), session))
        .map(table -> trimPath(context, table.getName()));
  }
}
