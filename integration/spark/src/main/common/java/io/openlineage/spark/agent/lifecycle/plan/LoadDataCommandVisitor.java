package io.openlineage.spark.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.util.PathUtils;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.QueryPlanVisitor;
import java.util.Collections;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.command.LoadDataCommand;

/**
 * {@link LogicalPlan} visitor that matches an {@link LoadDataCommandVisitor} and extracts the
 * output {@link OpenLineage.Dataset} being written.
 */
@Slf4j
public class LoadDataCommandVisitor
    extends QueryPlanVisitor<LoadDataCommand, OpenLineage.OutputDataset> {

  public LoadDataCommandVisitor(OpenLineageContext context) {
    super(context);
  }

  @Override
  public List<OpenLineage.OutputDataset> apply(LogicalPlan x) {
    LoadDataCommand command = (LoadDataCommand) x;
    return catalogTableFor(command.table())
        .map(
            table ->
                Collections.singletonList(
                    outputDataset().getDataset(PathUtils.fromCatalogTable(table), table.schema())))
        .orElseGet(Collections::emptyList);
  }
}
