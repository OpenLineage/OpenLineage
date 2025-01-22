/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle;

import io.micrometer.core.instrument.MeterRegistry;
import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.EventEmitter;
import io.openlineage.spark.agent.Versions;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.OpenLineageContext.OpenLineageContextBuilder;
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
import org.apache.spark.SparkContext$;
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

  /**
   * Spark can run multiple SQL queries in the context of streaming query being processed. In this
   * case we want to all the events to share the run id, job name and make sure that START and
   * COMPLETE events are sent only once.
   */
  private boolean streamingQueryMode = false;

  /** OpenLineage context to be reused when running within streaming queries. */
  @Getter private Optional<OpenLineageContext> streamingContext = Optional.empty();

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
    OpenLineageContext olContext =
        OpenLineageContext.builder()
            .sparkContext(SparkContext$.MODULE$.getActive().getOrElse(() -> null))
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

    return new RddExecutionContext(olContext, openLineageEventEmitter, runEventBuilder);
  }

  public Optional<ExecutionContext> createSparkSQLExecutionContext(long executionId) {
    QueryExecution queryExecution = SQLExecution.getQueryExecution(executionId);
    if (queryExecution == null) {
      log.error("Query execution is null: can't emit event for executionId {}", executionId);
      return Optional.empty();
    }

    SparkSession sparkSession = queryExecution.sparkSession();
    OpenLineageContextBuilder builder = OpenLineageContext.builder();
    boolean markStartEmitted = false;

    if (streamingQueryMode && streamingContext.isPresent()) {
      // pass some values from the existent streaming context
      builder
          .runUuid(streamingContext.get().getRunUuid())
          .applicationUuid(streamingContext.get().getApplicationUuid())
          .jobName(streamingContext.get().getJobName());

      markStartEmitted = true;
    }

    OpenLineageContext olContext =
        builder
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
    SparkSQLExecutionContext sqlExecutionContext =
        new SparkSQLExecutionContext(
            executionId, openLineageEventEmitter, olContext, runEventBuilder, streamingQueryMode);

    if (streamingQueryMode && !streamingContext.isPresent()) {
      streamingContext = Optional.of(olContext);
    }

    // as streaming context exists, START event was already emitted
    if (markStartEmitted) {
      sqlExecutionContext.setStartEmitted();
    }

    return Optional.of(sqlExecutionContext);
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
                  event.executionId(),
                  openLineageEventEmitter,
                  olContext,
                  runEventBuilder,
                  streamingQueryMode);
            });
  }

  public void clearStreamingQueryMode() {
    streamingContext = Optional.empty();
    streamingQueryMode = false;
  }

  public void startStreamingQueryMode() {
    streamingQueryMode = true;
    streamingContext = Optional.empty();
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
