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
import org.apache.spark.sql.execution.command.TruncateTableCommand;

/**
 * {@link LogicalPlan} visitor that matches an {@link TruncateTableCommand} and extracts the output
 * {@link OpenLineage.Dataset} being written.
 */
public class TruncateTableCommandVisitor
    extends QueryPlanVisitor<TruncateTableCommand, OpenLineage.Dataset> {

  private final SparkSession sparkSession;

  public TruncateTableCommandVisitor(SparkSession sparkSession) {
    this.sparkSession = sparkSession;
  }

  @Override
  @SneakyThrows
  public List<OpenLineage.Dataset> apply(LogicalPlan x) {
    TruncateTableCommand command = (TruncateTableCommand) x;

    if (sparkSession.sessionState().catalog().tableExists(command.tableName())) {
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
                      new TableStateChangeFacet(TableStateChangeFacet.StateChange.TRUNCATE))
                  .build()));

    } else {
      // table does not exist, cannot prepare an event
      return Collections.emptyList();
    }
  }
}
