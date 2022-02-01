package io.openlineage.spark.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.facets.TableStateChangeFacet;
import io.openlineage.spark.agent.facets.TableStateChangeFacet.StateChange;
import io.openlineage.spark.agent.util.PathUtils;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.QueryPlanVisitor;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.command.CreateDataSourceTableCommand;

/**
 * {@link LogicalPlan} visitor that matches an {@link CreateDataSourceTableCommand} and extracts the
 * output {@link OpenLineage.Dataset} being written.
 */
public class CreateDataSourceTableCommandVisitor
    extends QueryPlanVisitor<CreateDataSourceTableCommand, OpenLineage.OutputDataset> {

  public CreateDataSourceTableCommandVisitor(OpenLineageContext context) {
    super(context);
  }

  @Override
  public List<OpenLineage.OutputDataset> apply(LogicalPlan x) {
    CreateDataSourceTableCommand command = (CreateDataSourceTableCommand) x;
    CatalogTable catalogTable = command.table();
    Map<String, OpenLineage.DefaultDatasetFacet> facetMap =
        Collections.singletonMap("tableStateChange", new TableStateChangeFacet(StateChange.CREATE));

    return Collections.singletonList(
        outputDataset()
            .getDataset(PathUtils.fromCatalogTable(catalogTable), catalogTable.schema(), facetMap));
  }
}
