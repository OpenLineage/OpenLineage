/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark32.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.QueryPlanVisitor;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.command.RepairTableCommand;

public class RepairTableCommandVisitor
    extends QueryPlanVisitor<RepairTableCommand, OpenLineage.OutputDataset> {

  public RepairTableCommandVisitor(OpenLineageContext context) {
    super(context);
  }

  @Override
  public List<OpenLineage.OutputDataset> apply(LogicalPlan x) {
    RepairTableCommand cmd = (RepairTableCommand) x;
    Optional<CatalogTable> tableOption = catalogTableFor(cmd.tableName());

    return tableOption
        .map(
            catalogTable ->
                Collections.singletonList(
                    outputDataset()
                        .sparkDatasetBuilder()
                        .dataset(catalogTable)
                        .schema(catalogTable.schema())
                        .lifecycleStateChange(
                            OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange.ALTER)
                        .catalog(catalogTable.identifier())
                        .build()))
        .orElse(Collections.emptyList());
  }
}
