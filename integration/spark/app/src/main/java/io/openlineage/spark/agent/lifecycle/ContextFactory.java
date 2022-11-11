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
import java.util.Optional;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.SQLExecution;

public class ContextFactory {

  public final EventEmitter openLineageEventEmitter;
  private final OpenLineageEventHandlerFactory handlerFactory;
  private final Optional<String> appName;

  public ContextFactory(EventEmitter openLineageEventEmitter, Optional<String> appName) {
    this.openLineageEventEmitter = openLineageEventEmitter;
    this.appName = appName;
    handlerFactory = new InternalEventHandlerFactory();
  }

  public ExecutionContext createRddExecutionContext(int jobId) {
    return new RddExecutionContext(openLineageEventEmitter, appName);
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
                      .appName(appName)
                      .build();
              OpenLineageRunEventBuilder runEventBuilder =
                  new OpenLineageRunEventBuilder(olContext, handlerFactory);
              return new SparkSQLExecutionContext(
                  executionId, openLineageEventEmitter, olContext, runEventBuilder);
            });
  }
}
