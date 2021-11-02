package io.openlineage.spark3.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.lifecycle.plan.QueryPlanVisitor;
import io.openlineage.spark.agent.util.PlanUtils;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.catalyst.plans.logical.CreateTableAsSelect;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;

/**
 * {@link LogicalPlan} visitor that matches an {@link CreateTableAsSelect} and extracts the output
 * {@link OpenLineage.Dataset} being written.
 */
public class CreateTableAsSelectVisitor
    extends QueryPlanVisitor<CreateTableAsSelect, OpenLineage.Dataset> {

  @Override
  public List<OpenLineage.Dataset> apply(LogicalPlan x) {
    CreateTableAsSelect command = (CreateTableAsSelect) x;
    Path path = V2CatalogParser.getCatalogLocation(command.catalog());
    if (path == null) {
      return Collections.emptyList();
    }

    return Collections.singletonList(
        PlanUtils.getDataset(
            command.tableName().toString(),
            path.toString(),
            PlanUtils.datasetFacet(command.tableSchema(), path.toString())));
  }
}
