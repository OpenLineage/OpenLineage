package io.openlineage.spark.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.util.PathUtils;
import io.openlineage.spark.agent.util.PlanUtils;
import java.util.Collections;
import java.util.List;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.command.CreateDataSourceTableAsSelectCommand;

/**
 * {@link LogicalPlan} visitor that matches an {@link CreateDataSourceTableAsSelectCommand} and
 * extracts the output {@link OpenLineage.Dataset} being written.
 */
public class CreateDataSourceTableAsSelectCommandVisitor
    extends QueryPlanVisitor<CreateDataSourceTableAsSelectCommand, OpenLineage.Dataset> {

  @Override
  public List<OpenLineage.Dataset> apply(LogicalPlan x) {
    CreateDataSourceTableAsSelectCommand command = (CreateDataSourceTableAsSelectCommand) x;
    CatalogTable catalogTable = command.table();
    return Collections.singletonList(
        PlanUtils.getDataset(PathUtils.fromCatalogTable(catalogTable), catalogTable.schema()));
  }
}
