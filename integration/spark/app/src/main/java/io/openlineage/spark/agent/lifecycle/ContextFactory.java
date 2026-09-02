/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle;

import io.micrometer.core.instrument.MeterRegistry;
import io.openlineage.client.OpenLineage;
import io.openlineage.client.utils.UUIDUtils;
import io.openlineage.spark.agent.EventEmitter;
import io.openlineage.spark.agent.Spark4CompatUtils;
import io.openlineage.spark.agent.Versions;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.OpenLineageEventHandlerFactory;
import io.openlineage.spark.api.SparkOpenLineageConfig;
import io.openlineage.spark.api.Vendors;
import io.openlineage.spark.api.naming.JobNameBuilder;
import java.lang.reflect.InvocationTargetException;
import java.util.Collections;
import java.util.Optional;
import java.util.UUID;
import lombok.Getter;
import lombok.NonNull;
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
            .datasetBuilderFactory(DatasetBuilderFactoryProvider.getInstance())
            .build();

    String resolvedAppName = JobNameBuilder.buildApplicationName(olContext);
    this.openLineageEventEmitter.setApplicationJobName(resolvedAppName);

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
            .datasetBuilderFactory(DatasetBuilderFactoryProvider.getInstance())
            .build();

    OpenLineageRunEventBuilder runEventBuilder =
        new OpenLineageRunEventBuilder(olContext, handlerFactory);

    return new RddExecutionContext(olContext, openLineageEventEmitter, runEventBuilder);
  }

  /**
   * Creates a {@link SparkSQLExecutionContext} with a freshly generated run id when Spark still
   * exposes the {@link QueryExecution} of the execution through {@code
   * SQLExecution.getQueryExecution}.
   */
  public Optional<ExecutionContext> createSparkSQLExecutionContext(long executionId) {
    return createSparkSQLExecutionContext(executionId, UUIDUtils.generateNewUUID());
  }

  /**
   * Creates a {@link PendingSparkSQLExecutionContext} that buffers the start callback of a SQL
   * execution whose `QueryExecution` mapping is no longer available. The pending context is
   * upgraded, with the run id it generates, as soon as a query execution can be resolved again.
   */
  public ExecutionContext createPendingSparkSQLExecutionContext(
      long executionId, long rootExecutionId) {
    return new PendingSparkSQLExecutionContext(this, executionId, rootExecutionId);
  }

  /**
   * Creates a {@link SparkSQLExecutionContext} for the given run id. Returns an empty result when
   * Spark already removed the execution's temporary `QueryExecution` mapping; a {@link
   * PendingSparkSQLExecutionContext} can buffer the callback and use this method to upgrade once
   * the mapping is available again.
   */
  public Optional<ExecutionContext> createSparkSQLExecutionContext(
      long executionId, @NonNull UUID runUuid) {
    QueryExecution queryExecution = SQLExecution.getQueryExecution(executionId);
    if (queryExecution == null) {
      log.debug(
          "Query execution is not available (yet) for executionId {} - run id {}",
          executionId,
          runUuid);
      return Optional.empty();
    }
    return Optional.of(sqlExecutionContext(executionId, runUuid, queryExecution));
  }

  /**
   * Creates a {@link SparkSQLExecutionContext} with a freshly generated run id from the mutable
   * `qe` field carried by {@link SparkListenerSQLExecutionEnd} in all supported Spark versions.
   */
  public Optional<ExecutionContext> createSparkSQLExecutionContext(
      SparkListenerSQLExecutionEnd event) {
    return createSparkSQLExecutionContext(event, UUIDUtils.generateNewUUID());
  }

  /**
   * Creates a {@link SparkSQLExecutionContext} for the given run id from the `qe` field carried by
   * {@link SparkListenerSQLExecutionEnd}. Used to upgrade a {@link PendingSparkSQLExecutionContext}
   * when the execution's mapping was removed before the start event was processed.
   */
  public Optional<ExecutionContext> createSparkSQLExecutionContext(
      SparkListenerSQLExecutionEnd event, @NonNull UUID runUuid) {
    return executionFromCompleteEvent(event)
        .map(queryExecution -> sqlExecutionContext(event.executionId(), runUuid, queryExecution));
  }

  private SparkSQLExecutionContext sqlExecutionContext(
      long executionId, UUID runUuid, QueryExecution queryExecution) {
    SparkSession sparkSession = Spark4CompatUtils.getSparkSession(queryExecution);
    OpenLineageContext olContext =
        OpenLineageContext.builder()
            .sparkSession(sparkSession)
            .sparkContext(sparkSession.sparkContext())
            .openLineage(new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI))
            .queryExecution(queryExecution)
            .runUuid(runUuid)
            .customEnvironmentVariables(
                this.openLineageEventEmitter
                    .getCustomEnvironmentVariables()
                    .orElse(Collections.emptyList()))
            .vendors(Vendors.getVendors())
            .meterRegistry(meterRegistry)
            .openLineageConfig(config)
            .sparkExtensionVisitorWrapper(new SparkOpenLineageExtensionVisitorWrapper(config))
            .datasetBuilderFactory(DatasetBuilderFactoryProvider.getInstance())
            .build();
    OpenLineageRunEventBuilder runEventBuilder =
        new OpenLineageRunEventBuilder(olContext, handlerFactory);
    return new SparkSQLExecutionContext(
        executionId, openLineageEventEmitter, olContext, runEventBuilder);
  }

  public void close() {
    openLineageEventEmitter.close();
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
