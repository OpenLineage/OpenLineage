package openlineage.spark.agent.lifecycle;

import lombok.AllArgsConstructor;
import openlineage.spark.agent.OpenLineageSparkContext;
import openlineage.spark.agent.lifecycle.plan.InputDatasetVisitors;
import openlineage.spark.agent.lifecycle.plan.OutputDatasetVisitors;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.execution.SQLExecution;

@AllArgsConstructor
public class ContextFactory {
  public final OpenLineageSparkContext sparkContext;

  public void close() {
    sparkContext.close();
  }

  public RddExecutionContext createRddExecutionContext(int jobId) {
    return new RddExecutionContext(jobId, sparkContext);
  }

  public SparkSQLExecutionContext createSparkSQLExecutionContext(long executionId) {
    SQLContext sqlContext = SQLExecution.getQueryExecution(executionId).sparkPlan().sqlContext();
    InputDatasetVisitors inputDatasetVisitors =
        new InputDatasetVisitors(sqlContext, sparkContext);
    OutputDatasetVisitors outputDatasetVisitors =
        new OutputDatasetVisitors(sqlContext, inputDatasetVisitors);
    return new SparkSQLExecutionContext(
        executionId, sparkContext, outputDatasetVisitors.get(), inputDatasetVisitors.get());
  }
}
