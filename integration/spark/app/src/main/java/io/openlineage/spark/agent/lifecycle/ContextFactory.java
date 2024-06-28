/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle;

import io.micrometer.core.instrument.MeterRegistry;
import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.EventEmitter;
import io.openlineage.spark.agent.Versions;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.OpenLineageEventHandlerFactory;
import io.openlineage.spark.api.SparkOpenLineageConfig;
import io.openlineage.spark.api.Vendors;
import java.lang.reflect.InvocationTargetException;
import java.util.Collections;
import java.util.Optional;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.reflect.MethodUtils;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.QueryExecution;
import org.apache.spark.sql.execution.SQLExecution;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd;

@Slf4j
public class ContextFactory {

  public final EventEmitter openLineageEventEmitter;
  @Getter private final MeterRegistry meterRegistry;
  @Getter private final SparkOpenLineageConfig config;
  private final OpenLineageEventHandlerFactory handlerFactory;

  public ContextFactory(
      EventEmitter openLineageEventEmitter,
      MeterRegistry meterRegistry,
      SparkOpenLineageConfig config) {
    this.openLineageEventEmitter = openLineageEventEmitter;
    this.meterRegistry = meterRegistry;
    this.config = config;
    handlerFactory = new InternalEventHandlerFactory();
  }

  public ExecutionContext createSparkApplicationExecutionContext(SparkContext sparkContext) {
    OpenLineageContext olContext =
        OpenLineageContext.builder()
            .sparkContext(sparkContext)
            .applicationUuid(this.openLineageEventEmitter.getApplicationRunId())
            .openLineage(new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI))
            .customEnvironmentVariables(
                this.openLineageEventEmitter
                    .getCustomEnvironmentVariables()
                    .orElse(Collections.emptyList()))
            .vendors(Vendors.getVendors())
            .meterRegistry(meterRegistry)
            .openLineageConfig(config)
            .sparkExtensionVisitorWrapper(new SparkOpenLineageExtensionVisitorWrapper(config))
            .build();
    OpenLineageRunEventBuilder runEventBuilder =
        new OpenLineageRunEventBuilder(olContext, handlerFactory);
    return new SparkApplicationExecutionContext(
        openLineageEventEmitter, olContext, runEventBuilder);
  }

  public ExecutionContext createRddExecutionContext(int jobId) {
    return new RddExecutionContext(openLineageEventEmitter);
  }

  public Optional<ExecutionContext> createSparkSQLExecutionContext(long executionId) {
    QueryExecution queryExecution = SQLExecution.getQueryExecution(executionId);
    if (queryExecution == null) {
      log.error("Query execution is null: can't emit event for executionId {}", executionId);
      return Optional.empty();
    }
    SparkSession sparkSession = queryExecution.sparkSession();
    OpenLineageContext olContext =
        OpenLineageContext.builder()
            .sparkSession(sparkSession)
            .sparkContext(sparkSession.sparkContext())
            .openLineage(new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI))
            .queryExecution(queryExecution)
            .customEnvironmentVariables(
                this.openLineageEventEmitter
                    .getCustomEnvironmentVariables()
                    .orElse(Collections.emptyList()))
            .vendors(Vendors.getVendors())
            .meterRegistry(meterRegistry)
            .openLineageConfig(config)
            .sparkExtensionVisitorWrapper(new SparkOpenLineageExtensionVisitorWrapper(config))
            .build();
    OpenLineageRunEventBuilder runEventBuilder =
        new OpenLineageRunEventBuilder(olContext, handlerFactory);
    return Optional.of(
        new SparkSQLExecutionContext(
            executionId, openLineageEventEmitter, olContext, runEventBuilder));
  }

  public Optional<ExecutionContext> createSparkSQLExecutionContext(
      SparkListenerSQLExecutionEnd event) {
    return executionFromCompleteEvent(event)
        .map(
            queryExecution -> {
              SparkSession sparkSession = queryExecution.sparkSession();
              OpenLineageContext olContext =
                  OpenLineageContext.builder()
                      .sparkSession(sparkSession)
                      .sparkContext(sparkSession.sparkContext())
                      .openLineage(new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI))
                      .queryExecution(queryExecution)
                      .customEnvironmentVariables(
                          this.openLineageEventEmitter
                              .getCustomEnvironmentVariables()
                              .orElse(Collections.emptyList()))
                      .vendors(Vendors.getVendors())
                      .meterRegistry(meterRegistry)
                      .openLineageConfig(config)
                      .sparkExtensionVisitorWrapper(
                          new SparkOpenLineageExtensionVisitorWrapper(config))
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
      return Optional.of((QueryExecution) MethodUtils.invokeMethod(event, "qe", (Object[]) null));
    } catch (NoSuchMethodException e) {
      return Optional.empty();
    } catch (IllegalAccessException | InvocationTargetException | ClassCastException e) {
      log.warn("Invoking qe method failed", e);
      return Optional.empty();
    }
  }
}
