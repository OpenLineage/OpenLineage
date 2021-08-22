package io.openlineage.spark.agent.lifecycle;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.OpenLineageContext;
import io.openlineage.spark.agent.lifecycle.plan.BigQueryNodeVisitor;
import io.openlineage.spark.agent.lifecycle.plan.CommandPlanVisitor;
import io.openlineage.spark.agent.lifecycle.plan.DatasetSourceVisitor;
import io.openlineage.spark.agent.lifecycle.plan.InputDatasetVisitors;
import io.openlineage.spark.agent.lifecycle.plan.LogicalRDDVisitor;
import io.openlineage.spark.agent.lifecycle.plan.LogicalRelationVisitor;
import io.openlineage.spark.agent.lifecycle.plan.OutputDatasetVisitors;
import java.util.ArrayList;
import java.util.List;
import lombok.AllArgsConstructor;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.SQLExecution;
import scala.PartialFunction;

@AllArgsConstructor
public class ContextFactory {
  public final OpenLineageContext sparkContext;

  public void close() {
    sparkContext.close();
  }

  public RddExecutionContext createRddExecutionContext(int jobId) {
    return new RddExecutionContext(jobId, sparkContext);
  }

  public SparkSQLExecutionContext createSparkSQLExecutionContext(long executionId) {
    SQLContext sqlContext = SQLExecution.getQueryExecution(executionId).sparkPlan().sqlContext();

    List<PartialFunction<LogicalPlan, List<OpenLineage.Dataset>>> commonDatasetVisitors =
        commonDatasetVisitors(sqlContext);

    InputDatasetVisitors inputDatasetVisitors = new InputDatasetVisitors(commonDatasetVisitors);
    OutputDatasetVisitors outputDatasetVisitors =
        new OutputDatasetVisitors(sqlContext, commonDatasetVisitors);

    return new SparkSQLExecutionContext(
        executionId, sparkContext, outputDatasetVisitors.get(), inputDatasetVisitors.get());
  }

  private List<PartialFunction<LogicalPlan, List<OpenLineage.Dataset>>> commonDatasetVisitors(
      SQLContext sqlContext) {
    List<PartialFunction<LogicalPlan, List<OpenLineage.Dataset>>> list = new ArrayList<>();
    list.add(new LogicalRelationVisitor(sqlContext.sparkContext(), sparkContext.getJobNamespace()));
    list.add(new DatasetSourceVisitor());
    list.add(new LogicalRDDVisitor());
    list.add(new CommandPlanVisitor(new ArrayList<>(list)));
    if (BigQueryNodeVisitor.hasBigQueryClasses()) {
      list.add(new BigQueryNodeVisitor(sqlContext));
    }
    return list;
  }
}
