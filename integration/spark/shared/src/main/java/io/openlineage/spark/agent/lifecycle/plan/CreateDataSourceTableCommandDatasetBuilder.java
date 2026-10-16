/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.api.AbstractQueryPlanOutputDatasetBuilder;
import io.openlineage.spark.api.OpenLineageContext;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.command.CreateDataSourceTableCommand;

/**
 * Dataset builder that matches an {@link CreateDataSourceTableCommand} and extracts the output
 * {@link OpenLineage.Dataset} being written.
 */
public class CreateDataSourceTableCommandDatasetBuilder
    extends AbstractQueryPlanOutputDatasetBuilder<CreateDataSourceTableCommand> {

  public CreateDataSourceTableCommandDatasetBuilder(OpenLineageContext context) {
    super(context, false);
  }

  @Override
  public boolean isDefinedAtLogicalPlan(LogicalPlan x) {
    return x instanceof CreateDataSourceTableCommand;
  }

  @Override
  public List<OpenLineage.OutputDataset> apply(CreateDataSourceTableCommand command) {
    if (!context.getSparkSession().isPresent()) {
      return Collections.emptyList();
    }
    CatalogTable catalogTable = command.table();
    return Collections.singletonList(
        outputDataset()
            .sparkDatasetBuilder()
            .dataset(catalogTable)
            .schema(catalogTable.schema())
            .lifecycleStateChange(
                OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange.CREATE)
            .build());
  }

  @Override
  public Optional<String> jobNameSuffix(CreateDataSourceTableCommand command) {
    return Optional.of(command.table())
        .map(t -> t.identifier())
        .map(i -> tableIdentifierToSuffix(i));
  }
}
