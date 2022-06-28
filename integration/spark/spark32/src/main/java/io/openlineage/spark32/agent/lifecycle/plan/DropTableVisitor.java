/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark32.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.util.DatasetIdentifier;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.QueryPlanVisitor;
import io.openlineage.spark32.agent.utils.PlanUtils3;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.catalyst.analysis.ResolvedTable;
import org.apache.spark.sql.catalyst.plans.logical.DropTable;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Slf4j
public class DropTableVisitor extends QueryPlanVisitor<DropTable, OpenLineage.OutputDataset> {

  public DropTableVisitor(@NonNull OpenLineageContext context) {
    super(context);
  }

  @Override
  public List<OpenLineage.OutputDataset> apply(LogicalPlan x) {
    ResolvedTable resolvedTable = ((ResolvedTable) ((DropTable) x).child());
    TableCatalog tableCatalog = resolvedTable.catalog();
    Map<String, String> tableProperties = resolvedTable.table().properties();
    Identifier identifier = resolvedTable.identifier();

    Optional<DatasetIdentifier> di =
        PlanUtils3.getDatasetIdentifier(context, tableCatalog, identifier, tableProperties);

    if (di.isPresent()) {
      return Collections.singletonList(
          outputDataset()
              .getDataset(
                  di.get(),
                  resolvedTable.schema(),
                  OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange.DROP));
    } else {
      return Collections.emptyList();
    }
  }
}
