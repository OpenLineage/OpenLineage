package io.openlineage.spark3.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.util.DatasetIdentifier;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.QueryPlanVisitor;
import io.openlineage.spark3.agent.utils.PlanUtils3;
import java.util.List;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.catalyst.plans.logical.CreateTableAsSelect;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;

/**
 * {@link LogicalPlan} visitor that matches an {@link CreateTableAsSelect} and extracts the output
 * {@link OpenLineage.Dataset} being written.
 */
@Slf4j
public class CreateTableAsSelectVisitor
    extends QueryPlanVisitor<CreateTableAsSelect, OpenLineage.OutputDataset> {

  public CreateTableAsSelectVisitor(OpenLineageContext context) {
    super(context);
  }

  @Override
  public List<OpenLineage.OutputDataset> apply(LogicalPlan x) {
    CreateTableAsSelect command = (CreateTableAsSelect) x;
    List<DatasetIdentifier> datasetIds =
        PlanUtils3.getDataset(
            context.getSparkSession().get(),
            command.catalog(),
            command.tableName(),
            ScalaConversionUtils.<String, String>fromMap(command.properties()));
    return datasetIds.stream()
        .map(id -> outputDataset().getDataset(id, command.tableSchema()))
        .collect(Collectors.toList());
  }
}
