package marquez.spark.agent.lifecycle;

import lombok.AllArgsConstructor;
import marquez.spark.agent.MarquezContext;
import marquez.spark.agent.lifecycle.plan.InputDatasetVisitors;
import marquez.spark.agent.lifecycle.plan.OutputDatasetVisitors;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.execution.SQLExecution;

@AllArgsConstructor
public class ContextFactory {
  public final MarquezContext marquezContext;

  public void close() {
    marquezContext.close();
  }

  public RddExecutionContext createRddExecutionContext(int jobId) {
    return new RddExecutionContext(jobId, marquezContext);
  }

  public SparkSQLExecutionContext createSparkSQLExecutionContext(long executionId) {
    SQLContext sqlContext = SQLExecution.getQueryExecution(executionId).sparkPlan().sqlContext();
    InputDatasetVisitors inputDatasetVisitors =
        new InputDatasetVisitors(sqlContext, marquezContext);
    OutputDatasetVisitors outputDatasetVisitors =
        new OutputDatasetVisitors(sqlContext, inputDatasetVisitors);
    return new SparkSQLExecutionContext(
        executionId, marquezContext, outputDatasetVisitors.get(), inputDatasetVisitors.get());
  }
}
