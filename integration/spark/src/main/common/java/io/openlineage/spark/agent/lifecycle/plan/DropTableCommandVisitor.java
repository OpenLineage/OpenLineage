package io.openlineage.spark.agent.lifecycle.plan;

import static io.openlineage.spark.agent.util.PlanUtils.datasourceFacet;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.facets.TableStateChangeFacet;
import io.openlineage.spark.agent.util.DatasetIdentifier;
import io.openlineage.spark.agent.util.PathUtils;
import io.openlineage.spark.agent.util.PlanUtils;
import java.util.Collections;
import java.util.List;
import lombok.SneakyThrows;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.command.DropTableCommand;

/**
 * {@link LogicalPlan} visitor that matches an {@link DropTableCommand} and extracts the output
 * {@link OpenLineage.Dataset} being written.
 */
public class DropTableCommandVisitor
    extends QueryPlanVisitor<DropTableCommand, OpenLineage.Dataset> {

  private final SparkSession sparkSession;

  public DropTableCommandVisitor(SparkSession sparkSession) {
    this.sparkSession = sparkSession;
  }

  @Override
  @SneakyThrows
  public List<OpenLineage.Dataset> apply(LogicalPlan x) {
    DropTableCommand command = (DropTableCommand) x;

    if (sparkSession.sessionState().catalog().tableExists(command.tableName())) {
      // prepare an event, table will be deleted soon
      CatalogTable table =
          sparkSession.sessionState().catalog().getTableMetadata(command.tableName());
      DatasetIdentifier datasetIdentifier = PathUtils.fromCatalogTable(table);

      return Collections.singletonList(
          PlanUtils.getDataset(
              datasetIdentifier,
              new OpenLineage.DatasetFacetsBuilder()
                  .schema(null)
                  .dataSource(datasourceFacet(datasetIdentifier.getNamespace()))
                  .put(
                      "tableStateChange",
                      new TableStateChangeFacet(TableStateChangeFacet.StateChange.DROP))
                  .build()));

    } else {
      // already deleted, do nothing
      return Collections.emptyList();
    }
  }
}
