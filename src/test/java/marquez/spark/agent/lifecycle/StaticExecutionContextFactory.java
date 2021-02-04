package marquez.spark.agent.lifecycle;

import java.net.URI;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import marquez.spark.agent.MarquezContext;

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
    SparkSQLExecutionContext sparksql =
        new SparkSQLExecutionContext(
            executionId,
            marquezContext,
            new StaticLogicalPlanTraverser(),
            new StaticDatasetPlanTraverser()) {
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

  class StaticLogicalPlanTraverser extends LogicalPlanFacetTraverser {
    @Override
    protected Object visitPathUri(URI uri) {
      return "data.txt";
    }
  }

  class StaticDatasetPlanTraverser extends DatasetLogicalPlanTraverser {
    @Override
    protected URI visitPathUri(URI uri) {
      return URI.create("gs://bucket/data.txt");
    }
  }
}
