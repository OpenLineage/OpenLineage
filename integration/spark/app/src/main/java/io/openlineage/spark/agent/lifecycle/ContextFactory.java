/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle;

import io.micrometer.core.instrument.MeterRegistry;
import io.openlineage.client.OpenLineage;
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
import lombok.AccessLevel;
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
   * Built once and shared across all execution contexts. Its constructor walks the classpath
   * (ServiceLoader, Thread.getAllStackTraces, reflective defineClass) to discover
   * OpenLineageExtensionProvider implementations. Doing that on every SQL execution start ran on
   * the Spark listener thread and, when classpath JARs live on S3, could deadlock against the
   * driver thread over the AWS HTTP connection-pool lock and the app classloader's URLClassPath
   * monitor. The discovered providers are static for the JVM lifetime, so a single immutable
   * instance is built here (at ContextFactory construction, i.e. application start) and reused
   * everywhere.
   */
  @Getter(AccessLevel.PACKAGE)
  private final SparkOpenLineageExtensionVisitorWrapper visitorWrapper;

  public ContextFactory(
      EventEmitter openLineageEventEmitter,
      MeterRegistry meterRegistry,
      SparkOpenLineageConfig config) {
    this.openLineageEventEmitter = openLineageEventEmitter;
    this.meterRegistry = meterRegistry;
    this.config = config;
    handlerFactory = new InternalEventHandlerFactory();
    this.visitorWrapper = new SparkOpenLineageExtensionVisitorWrapper(config);
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
            .sparkExtensionVisitorWrapper(visitorWrapper)
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
            .sparkExtensionVisitorWrapper(visitorWrapper)
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
    SparkSession sparkSession = Spark4CompatUtils.getSparkSession(queryExecution);
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
            .sparkExtensionVisitorWrapper(visitorWrapper)
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
              SparkSession sparkSession = Spark4CompatUtils.getSparkSession(queryExecution);
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
                      .sparkExtensionVisitorWrapper(visitorWrapper)
                      .build();
              OpenLineageRunEventBuilder runEventBuilder =
                  new OpenLineageRunEventBuilder(olContext, handlerFactory);
              return new SparkSQLExecutionContext(
                  event.executionId(), openLineageEventEmitter, olContext, runEventBuilder);
            });
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
