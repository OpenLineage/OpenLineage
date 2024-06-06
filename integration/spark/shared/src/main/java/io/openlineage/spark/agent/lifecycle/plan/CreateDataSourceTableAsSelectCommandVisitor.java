/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.util.PathUtils;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.QueryPlanVisitor;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.command.CreateDataSourceTableAsSelectCommand;
import org.apache.spark.sql.types.StructType;

/**
 * {@link LogicalPlan} visitor that matches an {@link CreateDataSourceTableAsSelectCommand} and
 * extracts the output {@link OpenLineage.Dataset} being written.
 */
public class CreateDataSourceTableAsSelectCommandVisitor
    extends QueryPlanVisitor<CreateDataSourceTableAsSelectCommand, OpenLineage.OutputDataset> {

  public CreateDataSourceTableAsSelectCommandVisitor(OpenLineageContext context) {
    super(context);
  }

  @Override
  public List<OpenLineage.OutputDataset> apply(LogicalPlan x) {
    if (!context.getSparkSession().isPresent()) {
      return Collections.emptyList();
    }

    CreateDataSourceTableAsSelectCommand command = (CreateDataSourceTableAsSelectCommand) x;
    CatalogTable catalogTable = command.table();

    // Sometimes the schema is missing from the catalog (e.g., when the table doesn't yet exist)
    StructType schema = catalogTable.schema();
    if (schema.fields().length == 0 && command.query().schema().fields().length > 0) {
      schema = command.query().schema();
    }
    return Collections.singletonList(
        outputDataset()
            .getDataset(
                PathUtils.fromCatalogTable(catalogTable, context.getSparkSession().get()),
                schema,
                OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange.CREATE));
  }

  @Override
  public Optional<String> jobNameSuffix(CreateDataSourceTableAsSelectCommand command) {
    return Optional.of(tableIdentifierToSuffix(command.table().identifier()));
  }
}
