/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.util.PathUtils;
import io.openlineage.spark.api.AbstractOnCompleteOutputDatasetBuilder;
import io.openlineage.spark.api.OpenLineageContext;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.command.CreateTableCommand;

/**
 * Dataset builder that matches an {@link CreateTableCommand} and extracts the output {@link
 * OpenLineage.Dataset} being written.
 */
@Slf4j
public class CreateTableCommandDatasetBuilder
    extends AbstractOnCompleteOutputDatasetBuilder<CreateTableCommand> {

  public CreateTableCommandDatasetBuilder(OpenLineageContext context) {
    super(context, false);
  }

  @Override
  public boolean isDefinedAtLogicalPlan(LogicalPlan x) {
    return x instanceof CreateTableCommand;
  }

  @Override
  public List<OpenLineage.OutputDataset> apply(CreateTableCommand command) {
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
  public Optional<String> jobNameSuffix(CreateTableCommand command) {
    return context
        .getSparkSession()
        .map(session -> PathUtils.fromCatalogTable(command.table(), session))
        .map(table -> trimPath(context, table.getName()));
  }
}
