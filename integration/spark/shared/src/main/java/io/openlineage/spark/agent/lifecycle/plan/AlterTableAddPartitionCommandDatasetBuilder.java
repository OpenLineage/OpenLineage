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
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.command.AlterTableAddPartitionCommand;

@Slf4j
public class AlterTableAddPartitionCommandDatasetBuilder
    extends AbstractQueryPlanOutputDatasetBuilder<AlterTableAddPartitionCommand> {

  public AlterTableAddPartitionCommandDatasetBuilder(OpenLineageContext context) {
    super(context, false);
  }

  @Override
  public boolean isDefinedAtLogicalPlan(LogicalPlan x) {
    return x instanceof AlterTableAddPartitionCommand;
  }

  @Override
  public List<OpenLineage.OutputDataset> apply(AlterTableAddPartitionCommand x) {
    Optional<CatalogTable> tableOption = catalogTableFor(x.tableName());

    if (!tableOption.isPresent() || !context.getSparkSession().isPresent()) {
      return Collections.emptyList();
    }

    CatalogTable catalogTable = tableOption.get();

    // The generated datasets will not include partition nor location information
    return Collections.singletonList(
        outputDataset()
            .sparkDatasetBuilder()
            .dataset(catalogTable)
            .schema(catalogTable.schema())
            .lifecycleStateChange(
                OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange.ALTER)
            .build());
  }
}
