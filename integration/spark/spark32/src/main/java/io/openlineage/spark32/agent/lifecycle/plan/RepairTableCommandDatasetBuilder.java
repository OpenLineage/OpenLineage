/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark32.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.api.AbstractQueryPlanOutputDatasetBuilder;
import io.openlineage.spark.api.OpenLineageContext;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.command.RepairTableCommand;

/** Extracts the table repaired by a {@link RepairTableCommand} as an altered output dataset. */
public class RepairTableCommandDatasetBuilder
    extends AbstractQueryPlanOutputDatasetBuilder<RepairTableCommand> {

  public RepairTableCommandDatasetBuilder(OpenLineageContext context) {
    super(context, false);
  }

  @Override
  public boolean isDefinedAtLogicalPlan(LogicalPlan x) {
    return x instanceof RepairTableCommand;
  }

  @Override
  public List<OpenLineage.OutputDataset> apply(RepairTableCommand cmd) {
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
