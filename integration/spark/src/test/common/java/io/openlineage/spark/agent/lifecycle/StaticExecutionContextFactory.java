package io.openlineage.spark.agent.lifecycle;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.InputDataset;
import io.openlineage.client.OpenLineage.OutputDataset;
import io.openlineage.spark.agent.EventEmitter;
import io.openlineage.spark.agent.OpenLineageSparkListener;
import io.openlineage.spark.agent.client.OpenLineageClient;
import io.openlineage.spark.api.OpenLineageContext;
import java.net.URI;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.QueryExecution;
import org.apache.spark.sql.execution.SQLExecution;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart;
import scala.PartialFunction;

/** Returns deterministic fields for contexts */
public class StaticExecutionContextFactory extends ContextFactory {
  public static final Semaphore semaphore = new Semaphore(1);

  public StaticExecutionContextFactory(EventEmitter sparkContext) {
    super(sparkContext);
  }

  /**
   * The {@link OpenLineageSparkListener} is invoked by a {@link org.apache.spark.util.ListenerBus}
   * on a separate thread. In order for the tests to know that the listener events have been
   * processed, they can invoke this method which will wait up to one second for the processing to
   * complete. After that
   *
   * @throws InterruptedException
   */
  public static void waitForExecutionEnd() throws InterruptedException, TimeoutException {
    boolean acquired = semaphore.tryAcquire(5, TimeUnit.SECONDS);
    if (!acquired) {
      throw new TimeoutException(
          "Unable to acquire permit within expected timeout- "
              + "OpenLineageSparkListener processing may not have completed correctly");
    }
    semaphore.release();
  }

  @Override
  public ExecutionContext createRddExecutionContext(int jobId) {
    RddExecutionContext rdd =
        new RddExecutionContext(
            OpenLineageContext.builder()
                .sparkContext(SparkContext.getOrCreate())
                .openLineage(new OpenLineage(OpenLineageClient.OPEN_LINEAGE_CLIENT_URI))
                .build(),
            jobId,
            openLineageEventEmitter) {
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
  public ExecutionContext createSparkSQLExecutionContext(long executionId) {
    return Optional.ofNullable(SQLExecution.getQueryExecution(executionId))
        .map(
            qe -> {
              SparkSession session = qe.sparkSession();
              SQLContext sqlContext = qe.sparkPlan().sqlContext();
              OpenLineageContext olContext =
                  OpenLineageContext.builder()
                      .sparkSession(Optional.of(session))
                      .sparkContext(sqlContext.sparkContext())
                      .openLineage(new OpenLineage(OpenLineageClient.OPEN_LINEAGE_CLIENT_URI))
                      .queryExecution(qe)
                      .build();

              VisitorFactory visitorFactory =
                  VisitorFactoryProvider.getInstance(SparkSession.active());

              List<PartialFunction<LogicalPlan, List<InputDataset>>> inputDatasets =
                  visitorFactory.getInputVisitors(olContext);
              olContext.getInputDatasetQueryPlanVisitors().addAll(inputDatasets);
              List<PartialFunction<LogicalPlan, List<OutputDataset>>> outputDatasets =
                  visitorFactory.getOutputVisitors(olContext);
              olContext.getOutputDatasetQueryPlanVisitors().addAll(outputDatasets);

              SparkSQLExecutionContext sparksql =
                  new SparkSQLExecutionContext(
                      executionId, openLineageEventEmitter, qe, olContext) {
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
                    executionId,
                    openLineageEventEmitter,
                    null,
                    OpenLineageContext.builder()
                        .sparkContext(SparkContext.getOrCreate())
                        .openLineage(new OpenLineage(OpenLineageClient.OPEN_LINEAGE_CLIENT_URI))
                        .build()));
  }

  public ExecutionContext createSparkSQLExecutionContext(
      Long executionId, EventEmitter emitter, QueryExecution qe, OpenLineageContext olContext) {
    return new SparkSQLExecutionContext(executionId, emitter, qe, olContext);
  }

  private static ZonedDateTime getZonedTime() {
    return ZonedDateTime.of(2021, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC);
  }
}
