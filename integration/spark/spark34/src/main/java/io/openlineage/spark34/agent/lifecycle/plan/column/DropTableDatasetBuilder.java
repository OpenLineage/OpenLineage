/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark34.agent.lifecycle.plan.column;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.api.AbstractQueryPlanOutputDatasetBuilder;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark3.agent.utils.PlanUtils3;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Optional;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.catalyst.analysis.ResolvedIdentifier;
import org.apache.spark.sql.catalyst.plans.logical.DropTable;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.connector.catalog.TableCatalog;

@Slf4j
public class DropTableDatasetBuilder extends AbstractQueryPlanOutputDatasetBuilder<DropTable> {

  public DropTableDatasetBuilder(@NonNull OpenLineageContext context) {
    super(context, false);
  }

  @Override
  public boolean isDefinedAtLogicalPlan(LogicalPlan x) {
    return (x instanceof DropTable) && (((DropTable) x).child() instanceof ResolvedIdentifier);
  }

  @Override
  public List<OpenLineage.OutputDataset> apply(DropTable dropTable) {
    ResolvedIdentifier resolvedIdentifier = ((ResolvedIdentifier) (dropTable).child());

    return Optional.of(resolvedIdentifier.catalog())
        .filter(catalogPlugin -> catalogPlugin instanceof TableCatalog)
        .map(TableCatalog.class::cast)
        .flatMap(
            catalogPlugin ->
                PlanUtils3.getDatasetIdentifier(
                    context, catalogPlugin, resolvedIdentifier.identifier(), new LinkedHashMap<>()))
        .map(
            di ->
                outputDataset()
                    .getDataset(
                        di,
                        resolvedIdentifier.schema(),
                        OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange.DROP))
        .map(d -> Collections.singletonList(d))
        .orElseGet(() -> Collections.emptyList());
  }
}
