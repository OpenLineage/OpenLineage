/* SPDX-License-Identifier: Apache-2.0 */

package io.openlineage.spark.app.agent;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.shared.agent.lifecycle.ContextFactory;
import io.openlineage.spark.shared.agent.JobMetricsHolder;
import io.openlineage.spark.shared.agent.Versions;
import io.openlineage.spark.shared.agent.lifecycle.ExecutionContext;
import io.openlineage.spark.shared.agent.util.ScalaConversionUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.*;
import org.apache.spark.rdd.RDD;
import org.apache.spark.scheduler.*;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart;
import scala.Function0;
import scala.Function1;
import scala.Option;

import java.io.PrintWriter;
import java.net.URISyntaxException;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.stream.Collectors;

import static io.openlineage.spark.app.agent.ArgumentParser.DEFAULTS;
import static io.openlineage.spark.shared.agent.util.ScalaConversionUtils.asJavaOptional;
import static io.openlineage.spark.shared.agent.util.SparkConfUtils.findSparkConfigKey;
import static io.openlineage.spark.shared.agent.util.SparkConfUtils.findSparkUrlParams;

@Slf4j
public class OpenLineageSparkListener extends org.apache.spark.scheduler.SparkListener {

  private static final Map<Long, ExecutionContext> sparkSqlExecutionRegistry =
      Collections.synchronizedMap(new HashMap<>());
  private static final Map<Integer, ExecutionContext> rddExecutionRegistry =
      Collections.synchronizedMap(new HashMap<>());
  public static final String SPARK_CONF_URL_KEY = "openlineage.url";
  public static final String SPARK_CONF_HOST_KEY = "openlineage.host";
  public static final String SPARK_CONF_API_VERSION_KEY = "openlineage.version";
  public static final String SPARK_CONF_NAMESPACE_KEY = "openlineage.namespace";
  public static final String SPARK_CONF_JOB_NAME_KEY = "openlineage.parentJobName";
  public static final String SPARK_CONF_PARENT_RUN_ID_KEY = "openlineage.parentRunId";
  public static final String SPARK_CONF_API_KEY = "openlineage.apiKey";
  public static final String SPARK_CONF_URL_PARAM_PREFIX = "openlineage.url.param";
  private static WeakHashMap<RDD<?>, Configuration> outputs = new WeakHashMap<>();
  private static ContextFactory contextFactory;
  private static JobMetricsHolder jobMetrics = JobMetricsHolder.getInstance();
  private final Function1<SparkSession, SparkContext> sparkContextFromSession =
      ScalaConversionUtils.toScalaFn(SparkSession::sparkContext);
  private final Function0<Option<SparkContext>> activeSparkContext =
      ScalaConversionUtils.toScalaFn(SparkContext$.MODULE$::getActive);

  /** called by the tests */
  public static void init(ContextFactory contextFactory) {
    OpenLineageSparkListener.contextFactory = contextFactory;
    clear();
  }

  @Override
  public void onOtherEvent(SparkListenerEvent event) {
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
    }
  }

  /** called by the SparkListener when a job starts */
  @Override
  public void onJobStart(SparkListenerJobStart jobStart) {
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
    jobMetrics.addJobStages(jobStart.jobId(), stages);

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
    ExecutionContext context = rddExecutionRegistry.remove(jobEnd.jobId());
    if (context != null) {
      context.end(jobEnd);
    }
    jobMetrics.cleanUp(jobEnd.jobId());
  }

  @Override
  public void onTaskEnd(SparkListenerTaskEnd taskEnd) {
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
    if (executionContext.isPresent()) {
      rddExecutionRegistry.put(jobId, executionContext.get());
    }
    return executionContext;
  }

  public static Configuration getConfigForRDD(RDD<?> rdd) {
    return outputs.get(rdd);
  }

  public static void emitError(Exception e) {
    OpenLineage ol = new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI);
    try {
      contextFactory.openLineageEventEmitter.emit(buildErrorLineageEvent(ol, errorRunFacet(e, ol)));
    } catch (Exception ex) {
      log.error("Could not emit open lineage on error", e);
    }
  }

  private static OpenLineage.RunFacets errorRunFacet(Exception e, OpenLineage ol) {
    OpenLineage.RunFacet errorFacet = ol.newRunFacet();
    ByteArrayOutputStream buffer = new ByteArrayOutputStream();
    e.printStackTrace(new PrintWriter(buffer, true));
    errorFacet.getAdditionalProperties().put("exception", buffer.toString());

    OpenLineage.RunFacetsBuilder runFacetsBuilder = ol.newRunFacetsBuilder();
    runFacetsBuilder.put("lineage.error", errorFacet);
    return runFacetsBuilder.build();
  }

  public static OpenLineage.RunEvent buildErrorLineageEvent(
      OpenLineage ol, OpenLineage.RunFacets runFacets) {
    return ol.newRunEventBuilder()
        .eventTime(ZonedDateTime.now())
        .run(
            ol.newRun(
                contextFactory.openLineageEventEmitter.getParentRunId().orElse(null), runFacets))
        .job(
            ol.newJobBuilder()
                .namespace(contextFactory.openLineageEventEmitter.getJobNamespace())
                .name(contextFactory.openLineageEventEmitter.getParentJobName())
                .build())
        .build();
  }

  private static void clear() {
    sparkSqlExecutionRegistry.clear();
    rddExecutionRegistry.clear();
    outputs.clear();
  }

  @Override
  public void onApplicationEnd(SparkListenerApplicationEnd applicationEnd) {
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
    if (contextFactory != null) {
      return;
    }
    SparkEnv sparkEnv = SparkEnv$.MODULE$.get();
    if (sparkEnv != null) {
      try {
        ArgumentParser args = parseConf(sparkEnv.conf());
        contextFactory = new ContextFactory(new EventEmitter(args));
      } catch (URISyntaxException e) {
        log.error("Unable to parse open lineage endpoint. Lineage events will not be collected", e);
      }
    } else {
      log.warn(
          "Open lineage listener instantiated, but no configuration could be found. "
              + "Lineage events will not be collected");
    }
  }

  private ArgumentParser parseConf(SparkConf conf) {
    Optional<String> url = findSparkConfigKey(conf, SPARK_CONF_URL_KEY);
    if (url.isPresent()) {
      return ArgumentParser.parse(url.get());
    } else {
      String host = findSparkConfigKey(conf, SPARK_CONF_HOST_KEY, DEFAULTS.getHost());
      String version = findSparkConfigKey(conf, SPARK_CONF_API_VERSION_KEY, DEFAULTS.getVersion());
      String namespace =
          findSparkConfigKey(conf, SPARK_CONF_NAMESPACE_KEY, DEFAULTS.getNamespace());
      String jobName = findSparkConfigKey(conf, SPARK_CONF_JOB_NAME_KEY, DEFAULTS.getJobName());
      String runId =
          findSparkConfigKey(conf, SPARK_CONF_PARENT_RUN_ID_KEY, DEFAULTS.getParentRunId());
      Optional<String> apiKey =
          findSparkConfigKey(conf, SPARK_CONF_API_KEY).filter(str -> !str.isEmpty());
      Optional<Map<String, String>> urlParams =
          findSparkUrlParams(conf, SPARK_CONF_URL_PARAM_PREFIX);
      return new ArgumentParser(host, version, namespace, jobName, runId, apiKey, urlParams);
    }
  }
}
