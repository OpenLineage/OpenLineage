/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent;

import static io.openlineage.spark.agent.lifecycle.ExecutionContext.CAMEL_TO_SNAKE_CASE;
import static io.openlineage.spark.agent.util.ScalaConversionUtils.asJavaOptional;
import static io.openlineage.spark.agent.util.TimeUtils.toZonedTime;

import io.openlineage.client.Environment;
import io.openlineage.client.OpenLineage;
import io.openlineage.client.circuitBreaker.CircuitBreaker;
import io.openlineage.client.circuitBreaker.CircuitBreakerFactory;
import io.openlineage.spark.agent.lifecycle.ContextFactory;
import io.openlineage.spark.agent.lifecycle.ExecutionContext;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import java.io.PrintWriter;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.output.ByteArrayOutputStream;
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
import scala.Function0;
import scala.Function1;
import scala.Option;

@Slf4j
public class OpenLineageSparkListener extends org.apache.spark.scheduler.SparkListener {

  private static final Map<Long, ExecutionContext> sparkSqlExecutionRegistry =
      Collections.synchronizedMap(new HashMap<>());
  private static final Map<Integer, ExecutionContext> rddExecutionRegistry =
      Collections.synchronizedMap(new HashMap<>());
  private static WeakHashMap<RDD<?>, Configuration> outputs = new WeakHashMap<>();
  private static ContextFactory contextFactory;
  private static JobMetricsHolder jobMetrics = JobMetricsHolder.getInstance();
  private final Function1<SparkSession, SparkContext> sparkContextFromSession =
      ScalaConversionUtils.toScalaFn(SparkSession::sparkContext);
  private final Function0<Option<SparkContext>> activeSparkContext =
      ScalaConversionUtils.toScalaFn(SparkContext$.MODULE$::getActive);
  private static Optional<CircuitBreaker> circuitBreaker = Optional.empty();

  String sparkVersion = package$.MODULE$.SPARK_VERSION();

  private static final boolean isDisabled = checkIfDisabled();

  /** called by the tests */
  public static void init(ContextFactory contextFactory) {
    OpenLineageSparkListener.contextFactory = contextFactory;
    clear();
  }

  @Override
  public void onOtherEvent(SparkListenerEvent event) {
    if (isDisabled) {
      return;
    }
    initializeContextFactoryIfNotInitialized();
    if (isCircuitBreakerClosed()) {
      log.warn("CircuitBreaker has stopped emitting OpenLineage event.");
      return;
    }
    if (event instanceof SparkListenerSQLExecutionStart) {
      sparkSQLExecStart((SparkListenerSQLExecutionStart) event);
    } else if (event instanceof SparkListenerSQLExecutionEnd) {
      sparkSQLExecEnd((SparkListenerSQLExecutionEnd) event);
    }
  }

  /** called by the SparkListener when a spark-sql (Dataset api) execution starts */
  private static void sparkSQLExecStart(SparkListenerSQLExecutionStart startEvent) {
    getSparkSQLExecutionContext(startEvent.executionId())
        .ifPresent(context -> context.start(startEvent));
  }

  /** called by the SparkListener when a spark-sql (Dataset api) execution ends */
  private static void sparkSQLExecEnd(SparkListenerSQLExecutionEnd endEvent) {
    ExecutionContext context = sparkSqlExecutionRegistry.remove(endEvent.executionId());
    if (context != null) {
      context.end(endEvent);
    } else {
      contextFactory.createSparkSQLExecutionContext(endEvent).ifPresent(c -> c.end(endEvent));
    }
  }

  /** called by the SparkListener when a job starts */
  @Override
  public void onJobStart(SparkListenerJobStart jobStart) {
    if (isDisabled) {
      return;
    }
    initializeContextFactoryIfNotInitialized();
    if (isCircuitBreakerClosed()) {
      log.warn("CircuitBreaker has stopped emitting OpenLineage event.");
      return;
    }
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

    if (sparkVersion.startsWith("3")) {
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
              context.start(jobStart);
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
    if (isCircuitBreakerClosed()) {
      log.warn("CircuitBreaker has stopped emitting OpenLineage event.");
      return;
    }
    ExecutionContext context = rddExecutionRegistry.remove(jobEnd.jobId());
    if (context != null) {
      context.end(jobEnd);
    }
    if (sparkVersion.startsWith("3")) {
      jobMetrics.cleanUp(jobEnd.jobId());
    }
  }

  @Override
  public void onTaskEnd(SparkListenerTaskEnd taskEnd) {
    if (isDisabled || sparkVersion.startsWith("2")) {
      return;
    }
    if (isCircuitBreakerClosed()) {
      log.warn("CircuitBreaker has stopped emitting OpenLineage event.");
      return;
    }
    jobMetrics.addMetrics(taskEnd.stageId(), taskEnd.taskMetrics());
  }

  public static Optional<ExecutionContext> getSparkSQLExecutionContext(long executionId) {
    return Optional.ofNullable(
        sparkSqlExecutionRegistry.computeIfAbsent(
            executionId,
            (e) -> contextFactory.createSparkSQLExecutionContext(executionId).orElse(null)));
  }

  public static Optional<ExecutionContext> getExecutionContext(int jobId) {
    return Optional.ofNullable(
        rddExecutionRegistry.computeIfAbsent(
            jobId, (e) -> contextFactory.createRddExecutionContext(jobId)));
  }

  public static Optional<ExecutionContext> getExecutionContext(int jobId, long executionId) {
    Optional<ExecutionContext> executionContext = getSparkSQLExecutionContext(executionId);
    executionContext.ifPresent(context -> rddExecutionRegistry.put(jobId, context));
    return executionContext;
  }

  public static Configuration getConfigForRDD(RDD<?> rdd) {
    return outputs.get(rdd);
  }

  @SuppressWarnings(
      "PMD") // javadoc -> Closing a ByteArrayOutputStream has no effect. The methods in this class
  // can be called after the stream has been closed without generating an IOException.
  private static OpenLineage.RunFacets errorRunFacet(Exception e, OpenLineage ol) {
    OpenLineage.RunFacet errorFacet = ol.newRunFacet();
    ByteArrayOutputStream buffer = new ByteArrayOutputStream();
    e.printStackTrace(new PrintWriter(buffer, true));
    errorFacet.getAdditionalProperties().put("exception", buffer.toString());

    OpenLineage.RunFacetsBuilder runFacetsBuilder = ol.newRunFacetsBuilder();
    runFacetsBuilder.put("lineage.error", errorFacet);
    return runFacetsBuilder.build();
  }

  private static void clear() {
    sparkSqlExecutionRegistry.clear();
    rddExecutionRegistry.clear();
    outputs.clear();
  }

  @Override
  public void onApplicationEnd(SparkListenerApplicationEnd applicationEnd) {
    if (isCircuitBreakerClosed()) {
      log.warn("CircuitBreaker has stopped emitting OpenLineage event.");
      return;
    }
    emitApplicationEndEvent(applicationEnd.time());
    close();
    super.onApplicationEnd(applicationEnd);
  }

  /** To close the underlying resources. */
  public static void close() {
    clear();
  }

  /**
   * Check the {@link SparkConf} for open lineage configuration.
   *
   * @param applicationStart
   */
  @Override
  public void onApplicationStart(SparkListenerApplicationStart applicationStart) {
    initializeContextFactoryIfNotInitialized(applicationStart.appName());
    if (isCircuitBreakerClosed()) {
      return;
    }
    emitApplicationStartEvent(applicationStart.time());
  }

  private void initializeContextFactoryIfNotInitialized() {
    if (contextFactory != null || isDisabled) {
      return;
    }
    asJavaOptional(activeSparkContext.apply())
        .ifPresent(context -> initializeContextFactoryIfNotInitialized(context.appName()));
  }

  private void initializeContextFactoryIfNotInitialized(String appName) {
    if (contextFactory != null || isDisabled) {
      return;
    }
    SparkEnv sparkEnv = SparkEnv$.MODULE$.get();
    if (sparkEnv == null) {
      log.warn(
          "Open lineage listener instantiated, but no configuration could be found. "
              + "Lineage events will not be collected");
      return;
    }
    initializeContextFactoryIfNotInitialized(sparkEnv.conf(), appName);
  }

  private void initializeContextFactoryIfNotInitialized(SparkConf sparkConf, String appName) {
    if (contextFactory != null || isDisabled) {
      return;
    }
    try {
      ArgumentParser args = ArgumentParser.parse(sparkConf);
      contextFactory = new ContextFactory(new EventEmitter(args, appName));
      circuitBreaker =
          Optional.ofNullable(args.getOpenLineageYaml().getCircuitBreaker())
              .map(config -> new CircuitBreakerFactory(config).build());
    } catch (URISyntaxException e) {
      log.error("Unable to parse open lineage endpoint. Lineage events will not be collected", e);
    }
  }

  private void emitApplicationEvent(Long time, OpenLineage.RunEvent.EventType eventType) {
    OpenLineage openLineage = new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI);
    EventEmitter emitter = contextFactory.openLineageEventEmitter;
    OpenLineage.RunBuilder runBuilder =
        openLineage.newRunBuilder().runId(emitter.getApplicationRunId());
    if (emitter.getParentRunId().isPresent()
        && emitter.getParentJobName().isPresent()
        && emitter.getParentJobNamespace().isPresent()) {
      runBuilder.facets(
          openLineage
              .newRunFacetsBuilder()
              .parent(
                  openLineage.newParentRunFacet(
                      openLineage.newParentRunFacetRun(emitter.getParentRunId().get()),
                      openLineage.newParentRunFacetJob(
                          emitter.getParentJobNamespace().get(), emitter.getParentJobName().get())))
              .build());
    }
    OpenLineage.RunEvent applicationEvent =
        openLineage
            .newRunEventBuilder()
            .eventType(eventType)
            .eventTime(toZonedTime(time))
            .job(
                openLineage
                    .newJobBuilder()
                    .namespace(emitter.getJobNamespace())
                    .name(
                        emitter
                            .getApplicationJobName()
                            .replaceAll(CAMEL_TO_SNAKE_CASE, "_$1")
                            .toLowerCase(Locale.ROOT))
                    .build())
            .run(runBuilder.build())
            .inputs(Collections.emptyList())
            .outputs(Collections.emptyList())
            .build();
    emitter.emit(applicationEvent);
  }

  private void emitApplicationStartEvent(Long time) {
    emitApplicationEvent(time, OpenLineage.RunEvent.EventType.START);
  }

  private void emitApplicationEndEvent(Long time) {
    emitApplicationEvent(time, OpenLineage.RunEvent.EventType.COMPLETE);
  }

  private boolean isCircuitBreakerClosed() {
    return circuitBreaker.map(CircuitBreaker::isClosed).orElse(false);
  }

  private static boolean checkIfDisabled() {
    String isDisabled = Environment.getEnvironmentVariable("OPENLINEAGE_DISABLED");
    return Boolean.parseBoolean(isDisabled);
  }
}
