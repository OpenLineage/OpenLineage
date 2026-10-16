/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.api.AbstractQueryPlanOutputDatasetBuilder;
import io.openlineage.spark.api.OpenLineageContext;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.command.AlterTableAddColumnsCommand;
import org.apache.spark.sql.types.StructField;

public class AlterTableAddColumnsCommandDatasetBuilder
    extends AbstractQueryPlanOutputDatasetBuilder<AlterTableAddColumnsCommand> {

  public AlterTableAddColumnsCommandDatasetBuilder(OpenLineageContext context) {
    super(context, false);
  }

  @Override
  public boolean isDefinedAtLogicalPlan(LogicalPlan x) {
    return x instanceof AlterTableAddColumnsCommand;
  }

  @Override
  public List<OpenLineage.OutputDataset> apply(AlterTableAddColumnsCommand x) {
    Optional<CatalogTable> tableOption = catalogTableFor(x.table());
    if (!tableOption.isPresent() || !context.getSparkSession().isPresent()) {
      return Collections.emptyList();
    }
    CatalogTable catalogTable = tableOption.get();

    List<StructField> tableColumns = Arrays.asList(catalogTable.schema().fields());
    List<StructField> addedColumns = ScalaConversionUtils.fromSeq(x.colsToAdd());

    if (tableColumns.containsAll(addedColumns)) {
      return Collections.singletonList(
          outputDataset()
              .sparkDatasetBuilder()
              .dataset(catalogTable)
              .schema(catalogTable.schema())
              .lifecycleStateChange(
                  OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange.ALTER)
              .build());
    } else {
      // apply triggered before applying the change - do not send an event
      return Collections.emptyList();
    }
  }
}
