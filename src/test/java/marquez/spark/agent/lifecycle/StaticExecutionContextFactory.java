package marquez.spark.agent.lifecycle;

import java.net.URI;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import marquez.spark.agent.MarquezContext;
import marquez.spark.agent.lifecycle.plan.InputDatasetVisitors;
import marquez.spark.agent.lifecycle.plan.OutputDatasetVisitors;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.execution.SQLExecution;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart;

/** Returns deterministic fields for contexts */
public class StaticExecutionContextFactory extends ContextFactory {
  public static final Semaphore semaphore = new Semaphore(1);

  public StaticExecutionContextFactory(MarquezContext marquezContext) {
    super(marquezContext);
  }

  /**
   * The {@link marquez.spark.agent.SparkListener} is invoked by a {@link
   * org.apache.spark.util.ListenerBus} on a separate thread. In order for the tests to know that
   * the listener events have been processed, they can invoke this method which will wait up to one
   * second for the processing to complete. After that
   *
   * @throws InterruptedException
   */
  public static void waitForExecutionEnd() throws InterruptedException, TimeoutException {
    boolean acquired = semaphore.tryAcquire(1, TimeUnit.SECONDS);
    if (!acquired) {
      throw new TimeoutException(
          "Unable to acquire permit within expected timeout- "
              + "SparkListener processing may not have completed correctly");
    }
    semaphore.release();
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
    return Optional.ofNullable(SQLExecution.getQueryExecution(executionId))
        .map(
            qe -> {
              SQLContext sqlContext = qe.sparkPlan().sqlContext();
              InputDatasetVisitors inputDatasetVisitors = new InputDatasetVisitors(sqlContext);
              OutputDatasetVisitors outputDatasetVisitors =
                  new OutputDatasetVisitors(sqlContext, inputDatasetVisitors);
              SparkSQLExecutionContext sparksql =
                  new SparkSQLExecutionContext(
                      executionId,
                      marquezContext,
                      outputDatasetVisitors.get(),
                      inputDatasetVisitors.get()) {
                    @Override
                    public ZonedDateTime toZonedTime(long time) {
                      return getZonedTime();
                    }

                    @Override
                    public void start(SparkListenerSQLExecutionStart startEvent) {
                      try {
                        semaphore.acquire();
                      } catch (InterruptedException e) {
                        throw new RuntimeException("Unable to acquire semaphore", e);
                      }
                      super.start(startEvent);
                    }

                    @Override
                    public void end(SparkListenerSQLExecutionEnd endEvent) {
                      try {
                        super.end(endEvent);
                      } finally {
                        // ALWAYS release the permit
                        semaphore.release();
                      }
                    }
                  };
              return sparksql;
            })
        .orElseGet(
            () ->
                new SparkSQLExecutionContext(
                    executionId, marquezContext, Collections.emptyList(), Collections.emptyList()));
  }

  private static ZonedDateTime getZonedTime() {
    return ZonedDateTime.of(2021, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC);
  }
}
