/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent;

import static io.openlineage.spark.agent.util.ScalaConversionUtils.asJavaOptional;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.openlineage.client.Environment;
import io.openlineage.client.OpenLineage.RunEvent.EventType;
import io.openlineage.client.OpenLineageConfig;
import io.openlineage.client.circuitBreaker.CircuitBreaker;
import io.openlineage.client.circuitBreaker.CircuitBreakerFactory;
import io.openlineage.client.circuitBreaker.NoOpCircuitBreaker;
import io.openlineage.client.metrics.MicrometerProvider;
import io.openlineage.client.utils.RuntimeUtils;
import io.openlineage.spark.agent.lifecycle.ContextFactory;
import io.openlineage.spark.agent.lifecycle.ExecutionContext;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.agent.util.SparkVersionUtils;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.SparkOpenLineageConfig;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.SparkContext$;
import org.apache.spark.SparkEnv;
import org.apache.spark.SparkEnv$;
import org.apache.spark.package$;
import org.apache.spark.rdd.RDD;
import org.apache.spark.scheduler.ActiveJob;
import org.apache.spark.scheduler.SparkListenerApplicationEnd;
import org.apache.spark.scheduler.SparkListenerApplicationStart;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.scheduler.SparkListenerJobEnd;
import org.apache.spark.scheduler.SparkListenerJobStart;
import org.apache.spark.scheduler.SparkListenerTaskEnd;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart;
import org.apache.spark.sql.streaming.StreamingQueryListener.QueryStartedEvent;
import org.apache.spark.sql.streaming.StreamingQueryListener.QueryTerminatedEvent;
import scala.Function0;
import scala.Function1;
import scala.Option;

@Slf4j
public class OpenLineageSparkListener extends org.apache.spark.scheduler.SparkListener {

  private static final Map<Long, ExecutionContext> sparkSqlExecutionRegistry =
      Collections.synchronizedMap(new HashMap<>());
  private static final Map<Integer, ExecutionContext> rddExecutionRegistry =
      Collections.synchronizedMap(new HashMap<>());
  private static final WeakHashMap<RDD<?>, Configuration> outputs = new WeakHashMap<>();
  private static ContextFactory contextFactory;
  private static final JobMetricsHolder jobMetrics = JobMetricsHolder.getInstance();
  private static final Function1<SparkSession, SparkContext> sparkContextFromSession =
      ScalaConversionUtils.toScalaFn(SparkSession::sparkContext);
  private static final Function0<Option<SparkContext>> activeSparkContext =
      ScalaConversionUtils.toScalaFn(SparkContext$.MODULE$::getActive);

  private static CircuitBreaker circuitBreaker = new NoOpCircuitBreaker();

  private static MeterRegistry meterRegistry;

  private static final String sparkVersion = package$.MODULE$.SPARK_VERSION();

  private final boolean isDisabled = checkIfDisabled();

  /**
   * Id of the last active job. Has to be stored within the listener, as some jobs use both
   * RddExecutionContext and SparkSQLExecutionContext. jobId is required for to collect job metrics
   * which are collected within RddExecutionContext but emitted within SparkSQLExecutionContext.
   */
  private Optional<Integer> activeJobId = Optional.empty();

  /**
   * called by the tests
   *
   * @param contextFactory context factory
   */
  public static void init(ContextFactory contextFactory) {
    OpenLineageSparkListener.contextFactory = contextFactory;
    meterRegistry = contextFactory.getMeterRegistry();
    clear();
  }

  @Override
  public void onOtherEvent(SparkListenerEvent event) {
    if (isDisabled) {
      return;
    }
    if (event instanceof SparkListenerSQLExecutionStart) {
      initializeContextFactoryIfNotInitialized();
      log.debug("onOtherEvent called with event type SparkListenerSQLExecutionStart: [{}].", event);
      sparkSQLExecStart((SparkListenerSQLExecutionStart) event);
    } else if (event instanceof SparkListenerSQLExecutionEnd) {
      initializeContextFactoryIfNotInitialized();
      log.debug("onOtherEvent called with event type SparkListenerSQLExecutionEnd: [{}].", event);
      sparkSQLExecEnd((SparkListenerSQLExecutionEnd) event);
    } else if (event instanceof QueryStartedEvent) {
      // after setting this all SQL queries' events will get the same runId
      log.debug("onOtherEvent called with event type QueryStartedEvent: [{}].", event);
      contextFactory.startStreamingQueryMode();
    } else if (event instanceof QueryTerminatedEvent) {
      log.debug("onOtherEvent called with event type QueryTerminatedEvent: [{}].", event);
      streamingQueryTerminated((QueryTerminatedEvent) event);
    } else {
      log.warn("onOtherEvent called with unsupported event: {}", event.getClass());
    }
  }

  /** called by the SparkListener when a spark-sql (Dataset api) execution starts */
  private void sparkSQLExecStart(SparkListenerSQLExecutionStart startEvent) {
    getSparkSQLExecutionContext(startEvent.executionId())
        .ifPresent(
            context -> {
              meterRegistry.counter("openlineage.spark.event.sql.start").increment();
              circuitBreaker.run(
                  () -> {
                    activeJobId.ifPresent(context::setActiveJobId);
                    context.start(startEvent);
                    return null;
                  });
            });
  }

  /** called by the SparkListener when a spark-sql (Dataset api) execution ends */
  private void sparkSQLExecEnd(SparkListenerSQLExecutionEnd endEvent) {
    log.debug("sparkSQLExecEnd with activeJobId {}", activeJobId);
    ExecutionContext context = sparkSqlExecutionRegistry.remove(endEvent.executionId());
    meterRegistry.counter("openlineage.spark.event.sql.end").increment();
    if (context != null) {
      circuitBreaker.run(
          () -> {
            activeJobId.ifPresent(context::setActiveJobId);
            context.end(endEvent);
            return null;
          });
    } else {
      contextFactory
          .createSparkSQLExecutionContext(endEvent)
          .ifPresent(
              c ->
                  circuitBreaker.run(
                      () -> {
                        activeJobId.ifPresent(c::setActiveJobId);
                        c.end(endEvent);
                        return null;
                      }));
    }
  }

  private void streamingQueryTerminated(QueryTerminatedEvent event) {
    if (isDisabled) {
      return;
    }
    Optional<OpenLineageContext> streamingContext = contextFactory.getStreamingContext();
    contextFactory.clearStreamingQueryMode();
    circuitBreaker.run(
        () -> {
          streamingContext.ifPresent(
              context ->
                  contextFactory.openLineageEventEmitter.emit(
                      context
                          .getOpenLineage()
                          .newRunEventBuilder()
                          .run(
                              context
                                  .getOpenLineage()
                                  .newRunBuilder()
                                  .runId(context.getRunUuid())
                                  .build())
                          .job(
                              context
                                  .getOpenLineage()
                                  .newJobBuilder()
                                  .namespace(
                                      contextFactory.openLineageEventEmitter.getJobNamespace())
                                  .name(context.getJobName())
                                  .build())
                          .eventType(
                              event.exception().isEmpty() ? EventType.COMPLETE : EventType.FAIL)
                          .build()));
          return null;
        });
  }

  /** called by the SparkListener when a job starts */
  @Override
  public void onJobStart(SparkListenerJobStart jobStart) {
    if (isDisabled) {
      return;
    }
    log.debug("onJobStart called [{}].", jobStart);
    activeJobId = Optional.of(jobStart.jobId());
    initializeContextFactoryIfNotInitialized();
    meterRegistry.counter("openlineage.spark.event.job.start").increment();
    Optional<ActiveJob> activeJob =
        asJavaOptional(
                SparkSession.getDefaultSession()
                    .map(sparkContextFromSession)
                    .orElse(activeSparkContext))
            .flatMap(
                ctx ->
                    Optional.ofNullable(ctx.dagScheduler())
                        .map(ds -> ds.jobIdToActiveJob().get(jobStart.jobId())))
            .flatMap(ScalaConversionUtils::asJavaOptional);
    Set<Integer> stages =
        ScalaConversionUtils.fromSeq(jobStart.stageIds()).stream()
            .map(Integer.class::cast)
            .collect(Collectors.toSet());

    if (SparkVersionUtils.isSpark3OrHigher(sparkVersion)) {
      jobMetrics.addJobStages(jobStart.jobId(), stages);
    }

    Optional.ofNullable(getSqlExecutionId(jobStart.properties()))
        .map(Optional::of)
        .orElseGet(
            () ->
                asJavaOptional(
                        SparkSession.getDefaultSession()
                            .map(sparkContextFromSession)
                            .orElse(activeSparkContext))
                    .flatMap(
                        ctx ->
                            Optional.ofNullable(ctx.dagScheduler())
                                .map(ds -> ds.jobIdToActiveJob().get(jobStart.jobId()))
                                .flatMap(ScalaConversionUtils::asJavaOptional))
                    .map(job -> getSqlExecutionId(job.properties())))
        .map(Long::parseLong)
        .map(id -> getExecutionContext(jobStart.jobId(), id))
        .orElseGet(() -> getExecutionContext(jobStart.jobId()))
        .ifPresent(
            context -> {
              // set it in the rddExecutionRegistry so jobEnd is called
              activeJob.ifPresent(context::setActiveJob);
              circuitBreaker.run(
                  () -> {
                    context.start(jobStart);
                    return null;
                  });
            });
  }

  private String getSqlExecutionId(Properties properties) {
    return properties.getProperty("spark.sql.execution.id");
  }

  /** called by the SparkListener when a job ends */
  @Override
  public void onJobEnd(SparkListenerJobEnd jobEnd) {
    if (isDisabled) {
      return;
    }
    log.debug("onJobEnd called [{}].", jobEnd);
    ExecutionContext context = rddExecutionRegistry.remove(jobEnd.jobId());
    meterRegistry.counter("openlineage.spark.event.job.end").increment();
    circuitBreaker.run(
        () -> {
          if (context != null) {
            context.end(jobEnd);
          }
          return null;
        });
    if (SparkVersionUtils.isSpark3OrHigher(sparkVersion)) {
      jobMetrics.cleanUp(jobEnd.jobId());
    }
  }

  @Override
  public void onTaskEnd(SparkListenerTaskEnd taskEnd) {
    if (isDisabled || sparkVersion.startsWith("2")) {
      return;
    }
    log.debug("onTaskEnd called [{}].", taskEnd);
    jobMetrics.addMetrics(taskEnd.stageId(), taskEnd.taskMetrics());
  }

  private static ExecutionContext getSparkApplicationExecutionContext() {
    Optional<SparkContext> sparkContext =
        asJavaOptional(
            SparkSession.getDefaultSession()
                .map(sparkContextFromSession)
                .orElse(activeSparkContext));
    return contextFactory.createSparkApplicationExecutionContext(sparkContext.orElse(null));
  }

  private static Optional<ExecutionContext> getSparkSQLExecutionContext(long executionId) {
    return Optional.ofNullable(
        sparkSqlExecutionRegistry.computeIfAbsent(
            executionId,
            (e) -> contextFactory.createSparkSQLExecutionContext(executionId).orElse(null)));
  }

  private static Optional<ExecutionContext> getExecutionContext(int jobId) {
    return Optional.ofNullable(
        rddExecutionRegistry.computeIfAbsent(
            jobId, (e) -> contextFactory.createRddExecutionContext(jobId)));
  }

  private static Optional<ExecutionContext> getExecutionContext(int jobId, long executionId) {
    Optional<ExecutionContext> executionContext = getSparkSQLExecutionContext(executionId);
    executionContext.ifPresent(context -> rddExecutionRegistry.put(jobId, context));
    return executionContext;
  }

  public static Configuration getConfigForRDD(RDD<?> rdd) {
    return outputs.get(rdd);
  }

  private static void clear() {
    sparkSqlExecutionRegistry.clear();
    rddExecutionRegistry.clear();
    outputs.clear();
  }

  @Override
  public void onApplicationEnd(SparkListenerApplicationEnd applicationEnd) {
    if (isDisabled) {
      return;
    }
    log.debug("onApplicationEnd called [{}].", applicationEnd);
    meterRegistry.counter("openlineage.spark.event.app.end").increment();
    meterRegistry
        .counter("openlineage.spark.event.app.end.memoryusage")
        .increment(RuntimeUtils.getMemoryFractionUsage());

    circuitBreaker.run(
        () -> {
          getSparkApplicationExecutionContext().end(applicationEnd);
          return null;
        });
    close();
    super.onApplicationEnd(applicationEnd);
  }

  /** To close the underlying resources. */
  public static void close() {
    clear();
  }

  @Override
  public void onApplicationStart(SparkListenerApplicationStart applicationStart) {
    if (isDisabled) {
      return;
    }
    log.debug("onApplicationStart called [{}].", applicationStart);
    initializeContextFactoryIfNotInitialized(applicationStart.appName());
    meterRegistry.counter("openlineage.spark.event.app.start").increment();
    meterRegistry
        .counter("openlineage.spark.event.app.start.memoryusage")
        .increment(RuntimeUtils.getMemoryFractionUsage());

    circuitBreaker.run(
        () -> {
          getSparkApplicationExecutionContext().start(applicationStart);
          return null;
        });
  }

  private void initializeContextFactoryIfNotInitialized() {
    if (contextFactory != null) {
      return;
    }
    asJavaOptional(activeSparkContext.apply())
        .ifPresent(context -> initializeContextFactoryIfNotInitialized(context.appName()));
  }

  private void initializeContextFactoryIfNotInitialized(String appName) {
    if (contextFactory != null) {
      return;
    }
    SparkEnv sparkEnv = SparkEnv$.MODULE$.get();
    if (sparkEnv == null) {
      log.warn(
          "OpenLineage listener instantiated, but no configuration could be found. "
              + "Lineage events will not be collected");
      return;
    }
    initializeContextFactoryIfNotInitialized(sparkEnv.conf(), appName);
  }

  private void initializeContextFactoryIfNotInitialized(SparkConf sparkConf, String appName) {
    if (contextFactory != null) {
      return;
    }
    try {
      SparkOpenLineageConfig config = ArgumentParser.parse(sparkConf);
      // Needs to be done before initializing OpenLineageClient
      initializeMetrics(config);
      contextFactory = new ContextFactory(new EventEmitter(config, appName), meterRegistry, config);
      circuitBreaker = new CircuitBreakerFactory(config.getCircuitBreaker()).build();
    } catch (URISyntaxException e) {
      log.error("Unable to parse OpenLineage endpoint. Lineage events will not be collected", e);
    }
  }

  private static void initializeMetrics(OpenLineageConfig<?> openLineageConfig) {
    meterRegistry =
        MicrometerProvider.addMeterRegistryFromConfig(openLineageConfig.getMetricsConfig());
    String disabledFacets;
    if (openLineageConfig.getFacetsConfig() != null
        && openLineageConfig.getFacetsConfig().getDeprecatedDisabledFacets() != null) {
      disabledFacets =
          String.join(";", openLineageConfig.getFacetsConfig().getDeprecatedDisabledFacets());
    } else {
      disabledFacets = "";
    }
    meterRegistry
        .config()
        .commonTags(
            Tags.of(
                Tag.of("openlineage.spark.integration.version", Versions.getVersion()),
                Tag.of("openlineage.spark.version", sparkVersion),
                Tag.of("openlineage.spark.disabled.facets", disabledFacets)));
    ((CompositeMeterRegistry) meterRegistry)
        .getRegistries()
        .forEach(
            r ->
                r.config()
                    .commonTags(
                        Tags.of(
                            Tag.of("openlineage.spark.integration.version", Versions.getVersion()),
                            Tag.of("openlineage.spark.version", sparkVersion),
                            Tag.of("openlineage.spark.disabled.facets", disabledFacets))));
  }

  private static boolean checkIfDisabled() {
    String isDisabled = Environment.getEnvironmentVariable("OPENLINEAGE_DISABLED");
    return Boolean.parseBoolean(isDisabled);
  }
}
