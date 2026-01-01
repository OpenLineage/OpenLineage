/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.dataset.DatasetCompositeFacetsBuilder;
import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.spark.agent.util.PlanUtils;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.QueryPlanVisitor;
import io.openlineage.spark3.agent.lifecycle.plan.catalog.CatalogUtils3;
import io.openlineage.spark3.agent.utils.PlanUtils3;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.catalyst.analysis.ResolvedTable;
import org.apache.spark.sql.catalyst.plans.logical.DropTable;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;

@Slf4j
public class DropTableVisitor extends QueryPlanVisitor<DropTable, OpenLineage.OutputDataset> {

  public DropTableVisitor(@NonNull OpenLineageContext context) {
    super(context);
  }

  @Override
  public boolean isDefinedAt(LogicalPlan x) {
    // differs for Spark versions 3.4 and higher
    return (x instanceof DropTable) && (((DropTable) x).child() instanceof ResolvedTable);
  }

  @Override
  public List<OpenLineage.OutputDataset> apply(LogicalPlan x) {
    DropTable dropTable = (DropTable) x;

    ResolvedTable resolvedTable = ((ResolvedTable) (dropTable).child());
    TableCatalog tableCatalog = resolvedTable.catalog();
    Map<String, String> tableProperties = resolvedTable.table().properties();
    Identifier identifier = resolvedTable.identifier();

    Optional<DatasetIdentifier> di =
        PlanUtils3.getDatasetIdentifier(context, tableCatalog, identifier, tableProperties);

    if (di.isPresent()) {
      DatasetCompositeFacetsBuilder builder = outputDataset().createCompositeFacetBuilder();
      CatalogUtils3.getCatalogDatasetFacet(context, tableCatalog, tableProperties)
          .ifPresent(
              catalogDatasetFacet ->
                  builder.getFacets().catalog(catalogDatasetFacet.getCatalogDatasetFacet()));
      builder
          .getFacets()
          .schema(PlanUtils.schemaFacet(context.getOpenLineage(), resolvedTable.schema()));
      builder
          .getFacets()
          .lifecycleStateChange(
              context
                  .getOpenLineage()
                  .newLifecycleStateChangeDatasetFacet(
                      OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange.DROP,
                      null));

      return Collections.singletonList(outputDataset().getDataset(di.get(), builder));
    } else {
      return Collections.emptyList();
    }
  }
}
