/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.util.PathUtils;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.QueryPlanVisitor;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.command.AlterTableAddPartitionCommand;

@Slf4j
public class AlterTableAddPartitionCommandVisitor
    extends QueryPlanVisitor<AlterTableAddPartitionCommand, OpenLineage.OutputDataset> {

  public AlterTableAddPartitionCommandVisitor(OpenLineageContext context) {
    super(context);
  }

  @Override
  public List<OpenLineage.OutputDataset> apply(LogicalPlan x) {
    Optional<CatalogTable> tableOption =
        catalogTableFor(((AlterTableAddPartitionCommand) x).tableName());

    if (!tableOption.isPresent() || !context.getSparkSession().isPresent()) {
      return Collections.emptyList();
    }

    CatalogTable catalogTable = tableOption.get();

    // The generated datasets will not include partition nor location information
    return Collections.singletonList(
        outputDataset()
            .getDataset(
                PathUtils.fromCatalogTable(catalogTable, context.getSparkSession().get()),
                catalogTable.schema()));
  }
}
