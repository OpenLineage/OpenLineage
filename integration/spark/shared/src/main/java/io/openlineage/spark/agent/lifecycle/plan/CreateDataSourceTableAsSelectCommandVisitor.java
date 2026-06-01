/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.QueryPlanVisitor;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.command.CreateDataSourceTableAsSelectCommand;
import org.apache.spark.sql.types.StructType;

@Slf4j
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
    return context
        .getSparkSession()
        .map(
            session -> {
              CreateDataSourceTableAsSelectCommand command =
                  (CreateDataSourceTableAsSelectCommand) x;
              CatalogTable catalogTable = command.table();

              // Sometimes the schema is missing from the catalog (e.g., when the table doesn't yet
              // exist). Use the catalog schema if it has fields, otherwise fall back to the query
              // schema.
              StructType querySchema = command.query().schema();
              StructType schema =
                  Optional.of(catalogTable.schema())
                      .filter(s -> s.fields().length > 0)
                      .orElse(querySchema);
              return Collections.singletonList(
                  outputDataset()
                      .sparkDatasetBuilder()
                      .dataset(catalogTable)
                      .schema(schema)
                      .lifecycleStateChange(LifecycleStateChange.CREATE)
                      .build());
            })
        .orElse(Collections.emptyList());
  }

  @Override
  public Optional<String> jobNameSuffix(CreateDataSourceTableAsSelectCommand command) {
    return Optional.of(tableIdentifierToSuffix(command.table().identifier()));
  }
}
