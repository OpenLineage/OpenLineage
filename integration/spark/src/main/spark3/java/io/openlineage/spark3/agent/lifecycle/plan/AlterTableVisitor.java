package io.openlineage.spark3.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.util.DatasetIdentifier;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.QueryPlanVisitor;
import io.openlineage.spark3.agent.utils.PlanUtils3;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import lombok.NonNull;
import org.apache.spark.sql.catalyst.plans.logical.AlterTable;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;

public class AlterTableVisitor extends QueryPlanVisitor<AlterTable, OpenLineage.OutputDataset> {

  public AlterTableVisitor(@NonNull OpenLineageContext context) {
    super(context);
  }

  @Override
  public List<OpenLineage.OutputDataset> apply(LogicalPlan x) {
    TableCatalog tableCatalog;
    AlterTable alterTable = ((AlterTable) x);
    tableCatalog = alterTable.catalog();

    Table table;
    try {
      table = alterTable.catalog().loadTable(alterTable.ident());
    } catch (Exception e) {
      return Collections.emptyList();
    }

    Optional<DatasetIdentifier> di =
        PlanUtils3.getDatasetIdentifier(
            context, tableCatalog, alterTable.ident(), table.properties());
    if (di.isPresent()) {
      return Collections.singletonList(outputDataset().getDataset(di.get(), table.schema()));
    } else {
      return Collections.emptyList();
    }
  }
}
