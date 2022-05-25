/* SPDX-License-Identifier: Apache-2.0 */

package io.openlineage.spark.agent.lifecycle;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.EventEmitter;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.OpenLineageEventHandlerFactory;
import java.util.Optional;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.SQLExecution;

public class ContextFactory {

  public final EventEmitter openLineageEventEmitter;
  private final OpenLineageEventHandlerFactory handlerFactory;

  public ContextFactory(EventEmitter openLineageEventEmitter) {
    this.openLineageEventEmitter = openLineageEventEmitter;
    handlerFactory = new InternalEventHandlerFactory();
  }

  public ExecutionContext createRddExecutionContext(int jobId) {
    OpenLineageContext olContext =
        OpenLineageContext.builder()
            .sparkSession(ScalaConversionUtils.asJavaOptional(SparkSession.getActiveSession()))
            .sparkContext(SparkContext.getOrCreate())
            .openLineage(new OpenLineage(EventEmitter.OPEN_LINEAGE_PRODUCER_URI))
            .build();
    return new RddExecutionContext(olContext, jobId, openLineageEventEmitter);
  }

  public Optional<ExecutionContext> createSparkSQLExecutionContext(long executionId) {
    return Optional.ofNullable(SQLExecution.getQueryExecution(executionId))
        .map(
            queryExecution -> {
              SparkSession sparkSession = queryExecution.sparkSession();
              OpenLineageContext olContext =
                  OpenLineageContext.builder()
                      .sparkSession(Optional.of(sparkSession))
                      .sparkContext(sparkSession.sparkContext())
                      .openLineage(new OpenLineage(EventEmitter.OPEN_LINEAGE_PRODUCER_URI))
                      .queryExecution(queryExecution)
                      .build();
              OpenLineageRunEventBuilder runEventBuilder =
                  new OpenLineageRunEventBuilder(olContext, handlerFactory);
              return new SparkSQLExecutionContext(
                  executionId, openLineageEventEmitter, olContext, runEventBuilder);
            });
  }
}
