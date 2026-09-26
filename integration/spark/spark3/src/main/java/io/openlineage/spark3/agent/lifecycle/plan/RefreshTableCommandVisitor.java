/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.QueryPlanVisitor;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.command.RefreshTableCommand;

public class RefreshTableCommandVisitor
    extends QueryPlanVisitor<RefreshTableCommand, OpenLineage.InputDataset> {

  public RefreshTableCommandVisitor(OpenLineageContext context) {
    super(context);
  }

  @Override
  public List<OpenLineage.InputDataset> apply(LogicalPlan x) {
    Optional<CatalogTable> tableOption = catalogTableFor(((RefreshTableCommand) x).tableIdent());
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
