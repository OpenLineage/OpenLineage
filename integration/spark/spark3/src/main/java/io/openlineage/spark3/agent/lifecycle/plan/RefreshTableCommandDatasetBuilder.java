/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.api.AbstractQueryPlanInputDatasetBuilder;
import io.openlineage.spark.api.OpenLineageContext;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.command.RefreshTableCommand;

/** Extracts the table refreshed by a {@link RefreshTableCommand} as an altered input dataset. */
public class RefreshTableCommandDatasetBuilder
    extends AbstractQueryPlanInputDatasetBuilder<RefreshTableCommand> {

  public RefreshTableCommandDatasetBuilder(OpenLineageContext context) {
    super(context, true);
  }

  @Override
  public boolean isDefinedAtLogicalPlan(LogicalPlan x) {
    return x instanceof RefreshTableCommand;
  }

  @Override
  public List<OpenLineage.InputDataset> apply(RefreshTableCommand command) {
    Optional<CatalogTable> tableOption = catalogTableFor(command.tableIdent());
    return tableOption
        .map(
            catalogTable ->
                Collections.singletonList(
                    inputDataset()
                        .sparkDatasetBuilder()
                        .dataset(catalogTable)
                        .schema(catalogTable.schema())
                        .lifecycleStateChange(
                            OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange.ALTER)
                        .build()))
        .orElseGet(() -> Collections.emptyList());
  }
}
