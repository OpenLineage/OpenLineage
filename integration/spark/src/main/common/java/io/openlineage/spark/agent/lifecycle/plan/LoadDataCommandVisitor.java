package io.openlineage.spark.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.util.PathUtils;
import io.openlineage.spark.agent.util.PlanUtils;
import java.util.Collections;
import java.util.List;
import lombok.SneakyThrows;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.command.LoadDataCommand;

/**
 * {@link LogicalPlan} visitor that matches an {@link LoadDataCommandVisitor} and extracts the
 * output {@link OpenLineage.Dataset} being written.
 */
public class LoadDataCommandVisitor extends QueryPlanVisitor<LoadDataCommand, OpenLineage.Dataset> {

  private final SparkSession sparkSession;

  public LoadDataCommandVisitor(SparkSession sparkSession) {
    this.sparkSession = sparkSession;
  }

  @SneakyThrows
  @Override
  public List<OpenLineage.Dataset> apply(LogicalPlan x) {
    LoadDataCommand command = (LoadDataCommand) x;
    CatalogTable table = sparkSession.sessionState().catalog().getTableMetadata(command.table());
    return Collections.singletonList(
        PlanUtils.getDataset(PathUtils.fromCatalogTable(table), table.schema()));
  }
}
