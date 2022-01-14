/* SPDX-License-Identifier: Apache-2.0 */

package io.openlineage.spark3.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.facets.TableStateChangeFacet;
import io.openlineage.spark.agent.util.DatasetIdentifier;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.QueryPlanVisitor;
import io.openlineage.spark3.agent.utils.PlanUtils3;
import java.util.Collections;
import java.util.HashMap;
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
import org.apache.spark.sql.types.StructType;

@Slf4j
public class DropTableVisitor extends QueryPlanVisitor<DropTable, OpenLineage.OutputDataset> {

  public DropTableVisitor(@NonNull OpenLineageContext context) {
    super(context);
  }

  @Override
  public List<OpenLineage.OutputDataset> apply(LogicalPlan x) {
    Map<String, OpenLineage.DefaultDatasetFacet> facetMap = new HashMap<>();
    ResolvedTable resolvedTable = ((ResolvedTable) ((DropTable) x).child());
    TableCatalog tableCatalog = resolvedTable.catalog();
    Map<String, String> tableProperties = resolvedTable.table().properties();
    Identifier identifier = resolvedTable.identifier();
    StructType schema = resolvedTable.schema();

    facetMap.put(
        "tableStateChange", new TableStateChangeFacet(TableStateChangeFacet.StateChange.DROP));

    Optional<DatasetIdentifier> di =
        PlanUtils3.getDatasetIdentifier(context, tableCatalog, identifier, tableProperties);

    if (di.isPresent()) {
      return Collections.singletonList(outputDataset().getDataset(di.get(), schema, facetMap));
    } else {
      return Collections.emptyList();
    }
  }
}
