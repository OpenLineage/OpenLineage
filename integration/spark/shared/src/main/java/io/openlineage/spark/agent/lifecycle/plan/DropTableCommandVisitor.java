/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.QueryPlanVisitor;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.command.DropTableCommand;

/**
 * {@link LogicalPlan} visitor that matches an {@link DropTableCommand} and extracts the output
 * {@link OpenLineage.Dataset} being written.
 */
@Slf4j
public class DropTableCommandVisitor
    extends QueryPlanVisitor<DropTableCommand, OpenLineage.OutputDataset> {

  public DropTableCommandVisitor(OpenLineageContext context) {
    super(context);
  }

  @Override
  public List<OpenLineage.OutputDataset> apply(LogicalPlan x) {
    DropTableCommand command = (DropTableCommand) x;
    Optional<CatalogTable> table = catalogTableFor(command.tableName());
    if (!table.isPresent() || !context.getSparkSession().isPresent()) {
      return Collections.emptyList();
    }
    return Collections.singletonList(
        outputDataset()
            .sparkDatasetBuilder()
            .dataset(table.get())
            .lifecycleStateChange(
                OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange.DROP)
            .build());
  }
}
