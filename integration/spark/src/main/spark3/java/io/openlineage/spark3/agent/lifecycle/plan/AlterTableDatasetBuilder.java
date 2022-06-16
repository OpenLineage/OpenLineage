/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.util.DatasetIdentifier;
import io.openlineage.spark.agent.util.PlanUtils;
import io.openlineage.spark.api.AbstractQueryPlanOutputDatasetBuilder;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark3.agent.lifecycle.plan.catalog.CatalogUtils3;
import io.openlineage.spark3.agent.utils.PlanUtils3;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import lombok.NonNull;
import org.apache.spark.sql.catalyst.plans.logical.AlterTable;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;

public class AlterTableDatasetBuilder extends AbstractQueryPlanOutputDatasetBuilder<AlterTable> {

  public AlterTableDatasetBuilder(@NonNull OpenLineageContext context) {
    super(context, false);
  }

  @Override
  public boolean isDefinedAtLogicalPlan(LogicalPlan x) {
    return x instanceof AlterTable;
  }

  @Override
  public List<OpenLineage.OutputDataset> apply(AlterTable alterTable) {
    TableCatalog tableCatalog = alterTable.catalog();

    Table table;
    try {
      table = alterTable.catalog().loadTable(alterTable.ident());
    } catch (Exception e) {
      return Collections.emptyList();
    }

    Optional<DatasetIdentifier> di =
        PlanUtils3.getDatasetIdentifier(
            context, tableCatalog, alterTable.ident(), table.properties());

    if (di.isPresent()) {
      OpenLineage openLineage = context.getOpenLineage();
      OpenLineage.DatasetFacetsBuilder builder =
          openLineage
              .newDatasetFacetsBuilder()
              .schema(PlanUtils.schemaFacet(openLineage, table.schema()))
              .dataSource(PlanUtils.datasourceFacet(openLineage, di.get().getNamespace()));

      Optional<String> datasetVersion =
          CatalogUtils3.getDatasetVersion(tableCatalog, alterTable.ident(), table.properties());
      datasetVersion.ifPresent(
          version -> builder.version(openLineage.newDatasetVersionDatasetFacet(version)));

      return Collections.singletonList(
          outputDataset().getDataset(di.get().getName(), di.get().getNamespace(), builder.build()));
    } else {
      return Collections.emptyList();
    }
  }
}
