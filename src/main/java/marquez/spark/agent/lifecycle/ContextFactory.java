package marquez.spark.agent.lifecycle;

import lombok.AllArgsConstructor;
import marquez.spark.agent.MarquezContext;

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
    return new SparkSQLExecutionContext(
        executionId,
        marquezContext,
        new LogicalPlanFacetTraverser(),
        new DatasetLogicalPlanTraverser());
  }
}
