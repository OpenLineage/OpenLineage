/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange;
import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.spark.agent.util.PathUtils;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.QueryPlanVisitor;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.command.AlterTableRenameCommand;

@Slf4j
public class AlterTableRenameCommandVisitor
    extends QueryPlanVisitor<AlterTableRenameCommand, OpenLineage.OutputDataset> {

  public AlterTableRenameCommandVisitor(OpenLineageContext context) {
    super(context);
  }

  @SneakyThrows
  @Override
  public List<OpenLineage.OutputDataset> apply(LogicalPlan x) {
    Optional<CatalogTable> tableOpt = catalogTableFor(((AlterTableRenameCommand) x).newName());
    if (!tableOpt.isPresent() || !context.getSparkSession().isPresent()) {
      return Collections.emptyList();
    }
    CatalogTable table = tableOpt.get();

    DatasetIdentifier di = PathUtils.fromCatalogTable(table, context.getSparkSession().get());

    AlterTableRenameCommand cmd = (AlterTableRenameCommand) x;
    String previousName = di.getName().replace(cmd.newName().table(), cmd.oldName().table());

    return Collections.singletonList(
        outputDataset()
            .sparkDatasetBuilder()
            .dataset(di)
            .schema(table.schema())
            .lifecycleStateChange(LifecycleStateChange.RENAME, previousName, di.getNamespace())
            .catalog(table.identifier())
            .build());
  }
}
