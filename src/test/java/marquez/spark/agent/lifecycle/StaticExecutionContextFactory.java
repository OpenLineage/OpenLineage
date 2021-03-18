package marquez.spark.agent.lifecycle;

import java.net.URI;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import marquez.spark.agent.MarquezContext;
import marquez.spark.agent.lifecycle.plan.InputDatasetVisitors;
import marquez.spark.agent.lifecycle.plan.OutputDatasetVisitors;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.execution.SQLExecution;

/** Returns deterministic fields for contexts */
public class StaticExecutionContextFactory extends ContextFactory {

  public StaticExecutionContextFactory(MarquezContext marquezContext) {
    super(marquezContext);
  }

  @Override
  public RddExecutionContext createRddExecutionContext(int jobId) {
    RddExecutionContext rdd =
        new RddExecutionContext(jobId, marquezContext) {
          @Override
          protected ZonedDateTime toZonedTime(long time) {
            return getZonedTime();
          }

          @Override
          protected URI getDatasetUri(URI pathUri) {
            return URI.create("gs://bucket/data.txt");
          }
        };
    return rdd;
  }

  @Override
  public SparkSQLExecutionContext createSparkSQLExecutionContext(long executionId) {
    SQLContext sqlContext = SQLExecution.getQueryExecution(executionId).sparkPlan().sqlContext();
    InputDatasetVisitors inputDatasetVisitors = new InputDatasetVisitors(sqlContext);
    OutputDatasetVisitors outputDatasetVisitors =
        new OutputDatasetVisitors(sqlContext, inputDatasetVisitors);
    SparkSQLExecutionContext sparksql =
        new SparkSQLExecutionContext(
            executionId, marquezContext, outputDatasetVisitors.get(), inputDatasetVisitors.get()) {
          @Override
          public ZonedDateTime toZonedTime(long time) {
            return getZonedTime();
          }
        };
    return sparksql;
  }

  private static ZonedDateTime getZonedTime() {
    return ZonedDateTime.of(2021, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC);
  }
}
