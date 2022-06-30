/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark32.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.util.DatasetIdentifier;
import io.openlineage.spark.agent.util.PlanUtils;
import io.openlineage.spark.api.AbstractQueryPlanOutputDatasetBuilder;
import io.openlineage.spark.api.DatasetFactory;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark32.agent.lifecycle.plan.catalog.CatalogUtils3;
import io.openlineage.spark32.agent.utils.PlanUtils3;
import lombok.NonNull;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.catalyst.analysis.ResolvedTable;
import org.apache.spark.sql.catalyst.analysis.UnresolvedTable;
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
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.types.StructType;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class AlterTableDatasetBuilder extends AbstractQueryPlanOutputDatasetBuilder<LogicalPlan> {


    public AlterTableDatasetBuilder(@NonNull OpenLineageContext context) {
        super(context, false);
    }

    @Override
    public boolean isDefinedAtLogicalPlan(LogicalPlan x) {
        return x instanceof CommentOnTable ||
                x instanceof SetTableLocation ||
                x instanceof SetTableProperties ||
                x instanceof UnsetTableProperties ||
                x instanceof AddColumns ||
                x instanceof ReplaceColumns ||
                x instanceof DropColumns ||
                x instanceof RenameColumn ||
                x instanceof AlterColumn;
    }

    @Override
    protected List<OpenLineage.OutputDataset> apply(SparkListenerEvent event, LogicalPlan alterTableCommand) {

        ResolvedTable resolvedTable = (ResolvedTable) ((AlterTableCommand)alterTableCommand).table();
        TableCatalog tableCatalog = resolvedTable.catalog();
        Map<String, String> tableProperties = resolvedTable.table().properties();
        Identifier identifier = resolvedTable.identifier();
        StructType schema = resolvedTable.table().schema();
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
                    CatalogUtils3.getDatasetVersion(tableCatalog, identifier, tableProperties);
            datasetVersion.ifPresent(
                    version -> builder.version(openLineage.newDatasetVersionDatasetFacet(version)));
        }

        CatalogUtils3.getTableProviderFacet(tableCatalog, tableProperties)
                .map(provider -> builder.put("tableProvider", provider));
        return Collections.singletonList(
                outputDataset().getDataset(di.get().getName(), di.get().getNamespace(), builder.build()));
    }

}
