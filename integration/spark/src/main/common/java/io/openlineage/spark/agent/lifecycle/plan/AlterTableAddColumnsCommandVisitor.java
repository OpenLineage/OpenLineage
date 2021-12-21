package io.openlineage.spark.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.util.PathUtils;
import io.openlineage.spark.agent.util.PlanUtils;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import lombok.SneakyThrows;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.command.AlterTableAddColumnsCommand;
import org.apache.spark.sql.types.StructField;
import scala.collection.JavaConversions;

public class AlterTableAddColumnsCommandVisitor
    extends QueryPlanVisitor<AlterTableAddColumnsCommand, OpenLineage.Dataset> {

  private final SparkSession sparkSession;

  public AlterTableAddColumnsCommandVisitor(SparkSession sparkSession) {
    this.sparkSession = sparkSession;
  }

  @Override
  @SneakyThrows
  public List<OpenLineage.Dataset> apply(LogicalPlan x) {
    CatalogTable catalogTable =
        sparkSession
            .sessionState()
            .catalog()
            .getTableMetadata(((AlterTableAddColumnsCommand) x).table());

    List<StructField> tableColumns = Arrays.asList(catalogTable.schema().fields());
    List<StructField> addedColumns =
        JavaConversions.seqAsJavaList(((AlterTableAddColumnsCommand) x).colsToAdd());

    if (tableColumns.containsAll(addedColumns)) {
      return Collections.singletonList(
          PlanUtils.getDataset(PathUtils.fromCatalogTable(catalogTable), catalogTable.schema()));
    } else {
      // apply triggered before applying the change - do not send an event
      return Collections.emptyList();
    }
  }
}
