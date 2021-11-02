package io.openlineage.spark.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.util.PlanUtils;
import java.util.Collections;
import java.util.List;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.command.CreateDataSourceTableCommand;

/**
 * {@link LogicalPlan} visitor that matches an {@link CreateDataSourceTableCommand} and extracts the
 * output {@link OpenLineage.Dataset} being written.
 */
public class CreateDataSourceTableCommandVisitor
    extends QueryPlanVisitor<CreateDataSourceTableCommand, OpenLineage.Dataset> {

  @Override
  public List<OpenLineage.Dataset> apply(LogicalPlan x) {
    CreateDataSourceTableCommand command = (CreateDataSourceTableCommand) x;
    CatalogTable catalogTable = command.table();
    return Collections.singletonList(PlanUtils.getDataset(catalogTable));
  }
}
