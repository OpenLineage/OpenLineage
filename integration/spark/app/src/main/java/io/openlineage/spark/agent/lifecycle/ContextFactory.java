/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.EventEmitter;
import io.openlineage.spark.agent.Versions;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.OpenLineageEventHandlerFactory;
import java.lang.reflect.InvocationTargetException;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.reflect.MethodUtils;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.QueryExecution;
import org.apache.spark.sql.execution.SQLExecution;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd;

@Slf4j
public class ContextFactory {

  public final EventEmitter openLineageEventEmitter;
  private final OpenLineageEventHandlerFactory handlerFactory;

  public ContextFactory(EventEmitter openLineageEventEmitter) {
    this.openLineageEventEmitter = openLineageEventEmitter;
    handlerFactory = new InternalEventHandlerFactory();
  }

  public ExecutionContext createRddExecutionContext(int jobId) {
    return new RddExecutionContext(openLineageEventEmitter);
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
                      .openLineage(new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI))
                      .queryExecution(queryExecution)
                      .customEnvironmentVariables(
                          this.openLineageEventEmitter.getCustomEnvironmentVariables())
                      .build();
              OpenLineageRunEventBuilder runEventBuilder =
                  new OpenLineageRunEventBuilder(olContext, handlerFactory);
              return new SparkSQLExecutionContext(
                  executionId, openLineageEventEmitter, olContext, runEventBuilder);
            });
  }

  public Optional<ExecutionContext> createSparkSQLExecutionContext(
      SparkListenerSQLExecutionEnd event) {
    return executionFromCompleteEvent(event)
        .map(
            queryExecution -> {
              SparkSession sparkSession = queryExecution.sparkSession();
              OpenLineageContext olContext =
                  OpenLineageContext.builder()
                      .sparkSession(Optional.of(sparkSession))
                      .sparkContext(sparkSession.sparkContext())
                      .openLineage(new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI))
                      .queryExecution(queryExecution)
                      .customEnvironmentVariables(
                          this.openLineageEventEmitter.getCustomEnvironmentVariables())
                      .build();
              OpenLineageRunEventBuilder runEventBuilder =
                  new OpenLineageRunEventBuilder(olContext, handlerFactory);
              return new SparkSQLExecutionContext(
                  event.executionId(), openLineageEventEmitter, olContext, runEventBuilder);
            });
  }

  public static Optional<QueryExecution> executionFromCompleteEvent(
      SparkListenerSQLExecutionEnd event) {
    try {
      return Optional.of((QueryExecution) MethodUtils.invokeMethod(event, "qe", null));
    } catch (NoSuchMethodException e) {
      return Optional.empty();
    } catch (IllegalAccessException | InvocationTargetException | ClassCastException e) {
      log.warn("Invoking qe method failed", e);
      return Optional.empty();
    }
  }
}
