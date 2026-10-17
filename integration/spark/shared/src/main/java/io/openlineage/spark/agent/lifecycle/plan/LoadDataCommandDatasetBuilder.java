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
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.command.LoadDataCommand;

/**
 * Dataset builder that matches an {@link LoadDataCommandDatasetBuilder} and extracts the output
 * {@link OpenLineage.Dataset} being written.
 */
@Slf4j
public class LoadDataCommandDatasetBuilder
    extends AbstractQueryPlanOutputDatasetBuilder<LoadDataCommand> {

  public LoadDataCommandDatasetBuilder(OpenLineageContext context) {
    super(context, false);
  }

  @Override
  public boolean isDefinedAtLogicalPlan(LogicalPlan x) {
    return x instanceof LoadDataCommand;
  }

  @Override
  public List<OpenLineage.OutputDataset> apply(LoadDataCommand command) {
    if (!context.getSparkSession().isPresent()) {
      return Collections.emptyList();
    }
    return catalogTableFor(command.table())
        .map(
            table ->
                Collections.singletonList(
                    outputDataset()
                        .sparkDatasetBuilder()
                        .dataset(table)
                        .schema(table.schema())
                        .build()))
        .orElseGet(Collections::emptyList);
  }

  @Override
  public Optional<String> jobNameSuffix(LoadDataCommand command) {
    return Optional.of(tableIdentifierToSuffix(command.table()));
  }
}
