package io.openlineage.spark.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.util.PathUtils;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.QueryPlanVisitor;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.command.AlterTableAddColumnsCommand;
import org.apache.spark.sql.types.StructField;
import scala.collection.JavaConversions;

public class AlterTableAddColumnsCommandVisitor
    extends QueryPlanVisitor<AlterTableAddColumnsCommand, OpenLineage.OutputDataset> {

  public AlterTableAddColumnsCommandVisitor(OpenLineageContext context) {
    super(context);
  }

  @Override
  public List<OpenLineage.OutputDataset> apply(LogicalPlan x) {
    Optional<CatalogTable> tableOption = catalogTableFor(((AlterTableAddColumnsCommand) x).table());
    if (!tableOption.isPresent()) {
      return Collections.emptyList();
    }
    CatalogTable catalogTable = tableOption.get();

    List<StructField> tableColumns = Arrays.asList(catalogTable.schema().fields());
    List<StructField> addedColumns =
        JavaConversions.seqAsJavaList(((AlterTableAddColumnsCommand) x).colsToAdd());

    if (tableColumns.containsAll(addedColumns)) {
      return Collections.singletonList(
          outputDataset()
              .getDataset(PathUtils.fromCatalogTable(catalogTable), catalogTable.schema()));
    } else {
      // apply triggered before applying the change - do not send an event
      return Collections.emptyList();
    }
  }
}
