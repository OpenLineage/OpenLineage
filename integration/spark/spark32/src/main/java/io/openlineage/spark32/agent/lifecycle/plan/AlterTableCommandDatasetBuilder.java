/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark32.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.util.DatasetIdentifier;
import io.openlineage.spark.agent.util.PlanUtils;
import io.openlineage.spark.api.AbstractQueryPlanOutputDatasetBuilder;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark3.agent.lifecycle.plan.catalog.CatalogUtils3;
import io.openlineage.spark3.agent.utils.PlanUtils3;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.ResolvedTable;
import org.apache.spark.sql.catalyst.plans.logical.AddColumns;
import org.apache.spark.sql.catalyst.plans.logical.AlterColumn;
import org.apache.spark.sql.catalyst.plans.logical.AlterTableCommand;
import org.apache.spark.sql.catalyst.plans.logical.CommentOnTable;
import org.apache.spark.sql.catalyst.plans.logical.DropColumns;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.RenameColumn;
import org.apache.spark.sql.catalyst.plans.logical.ReplaceColumns;
import org.apache.spark.sql.catalyst.plans.logical.SetTableLocation;
import org.apache.spark.sql.catalyst.plans.logical.SetTableProperties;
import org.apache.spark.sql.catalyst.plans.logical.UnsetTableProperties;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.types.StructType;

@Slf4j
public class AlterTableCommandDatasetBuilder
    extends AbstractQueryPlanOutputDatasetBuilder<LogicalPlan> {

  public AlterTableCommandDatasetBuilder(@NonNull OpenLineageContext context) {
    super(context, false);
  }

  @Override
  public boolean isDefinedAtLogicalPlan(LogicalPlan x) {
    return x instanceof CommentOnTable
        || x instanceof SetTableLocation
        || x instanceof SetTableProperties
        || x instanceof UnsetTableProperties
        || x instanceof AddColumns
        || x instanceof ReplaceColumns
        || x instanceof DropColumns
        || x instanceof RenameColumn
        || x instanceof AlterColumn;
  }

  @Override
  protected List<OpenLineage.OutputDataset> apply(
      SparkListenerEvent event, LogicalPlan alterTableCommand) {
    ResolvedTable resolvedTable = (ResolvedTable) ((AlterTableCommand) alterTableCommand).table();
    Table table;
    try {
      // resolvedTable has only old metadata (before alter)
      table = resolvedTable.catalog().loadTable(resolvedTable.identifier());
    } catch (NoSuchTableException e) {
      return Collections.emptyList();
    }
    TableCatalog tableCatalog = resolvedTable.catalog();
    Map<String, String> tableProperties = table.properties();
    Identifier identifier = resolvedTable.identifier();
    StructType schema = table.schema();
    OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange lifecycleStateChange =
        OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange.ALTER;

    Optional<DatasetIdentifier> di =
        PlanUtils3.getDatasetIdentifier(context, tableCatalog, identifier, tableProperties);

    if (!di.isPresent()) {
      return Collections.emptyList();
    }

    OpenLineage openLineage = context.getOpenLineage();
    OpenLineage.DatasetFacetsBuilder builder =
        openLineage
            .newDatasetFacetsBuilder()
            .schema(PlanUtils.schemaFacet(openLineage, schema))
            .lifecycleStateChange(
                openLineage.newLifecycleStateChangeDatasetFacet(lifecycleStateChange, null))
            .dataSource(PlanUtils.datasourceFacet(openLineage, di.get().getNamespace()));

    if (includeDatasetVersion(event)) {
      Optional<String> datasetVersion =
          CatalogUtils3.getDatasetVersion(
              context, resolvedTable.catalog(), resolvedTable.identifier(), table.properties());
      datasetVersion.ifPresent(
          version -> builder.version(openLineage.newDatasetVersionDatasetFacet(version)));
    }
    return Collections.singletonList(outputDataset().getDataset(di.get(), builder));
  }
}
