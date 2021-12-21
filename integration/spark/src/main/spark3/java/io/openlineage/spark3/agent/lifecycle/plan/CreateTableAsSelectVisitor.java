package io.openlineage.spark3.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.lifecycle.plan.QueryPlanVisitor;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark3.agent.utils.PlanUtils3;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.plans.logical.CreateTableAsSelect;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;

/**
 * {@link LogicalPlan} visitor that matches an {@link CreateTableAsSelect} and extracts the output
 * {@link OpenLineage.Dataset} being written.
 */
@Slf4j
public class CreateTableAsSelectVisitor
    extends QueryPlanVisitor<CreateTableAsSelect, OpenLineage.Dataset> {

  private final SparkSession sparkSession;

  public CreateTableAsSelectVisitor(SparkSession sparkSession) {
    this.sparkSession = sparkSession;
  }

  @Override
  public List<OpenLineage.Dataset> apply(LogicalPlan x) {
    CreateTableAsSelect command = (CreateTableAsSelect) x;
    return PlanUtils3.getDataset(
        sparkSession,
        command.catalog(),
        command.tableName(),
        ScalaConversionUtils.<String, String>fromMap(command.properties()),
        command.tableSchema());
  }
}
