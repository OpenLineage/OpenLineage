package io.openlineage.spark.agent.lifecycle;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.OpenLineageContext;
import io.openlineage.spark.agent.lifecycle.plan.BigQueryNodeVisitor;
import io.openlineage.spark.agent.lifecycle.plan.CommandPlanVisitor;
import io.openlineage.spark.agent.lifecycle.plan.LogicalRDDVisitor;
import io.openlineage.spark.agent.lifecycle.plan.LogicalRelationVisitor;
import io.openlineage.spark.agent.lifecycle.plan.QueryPlanVisitor;
import java.util.ArrayList;
import java.util.List;
import lombok.AllArgsConstructor;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.QueryExecution;
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
    QueryExecution queryExecution = SQLExecution.getQueryExecution(executionId);
    SQLContext sqlContext = queryExecution.sparkPlan().sqlContext();

    VisitorFactory visitorFactory = VisitorFactoryProvider.getInstance(SparkSession.active());

    List<QueryPlanVisitor<LogicalPlan, OpenLineage.InputDataset>> inputDatasets =
        visitorFactory.getInputVisitors(sqlContext, sparkContext.getJobNamespace());

    List<QueryPlanVisitor<LogicalPlan, OpenLineage.OutputDataset>> outputDatasets =
        visitorFactory.getOutputVisitors(sqlContext, sparkContext.getJobNamespace());

    return new SparkSQLExecutionContext(
        executionId, sparkContext, queryExecution, outputDatasets, inputDatasets);
  }

  private List<PartialFunction<LogicalPlan, List<OpenLineage.Dataset>>> commonDatasetVisitors(
      SQLContext sqlContext) {
    List<PartialFunction<LogicalPlan, List<OpenLineage.Dataset>>> list = new ArrayList<>();
    list.add(new LogicalRelationVisitor(sqlContext.sparkContext(), sparkContext.getJobNamespace()));
    list.add(new LogicalRDDVisitor());
    list.add(new CommandPlanVisitor(new ArrayList<>(list)));
    if (BigQueryNodeVisitor.hasBigQueryClasses()) {
      list.add(new BigQueryNodeVisitor(sqlContext));
    }
    return list;
  }
}
