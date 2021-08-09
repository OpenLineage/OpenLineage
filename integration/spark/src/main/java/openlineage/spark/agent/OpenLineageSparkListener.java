package openlineage.spark.agent;

import static openlineage.spark.agent.ArgumentParser.DEFAULTS;
import static openlineage.spark.agent.lifecycle.plan.ScalaConversionUtils.asJavaOptional;
import static openlineage.spark.agent.lifecycle.plan.ScalaConversionUtils.toScalaFn;

import io.openlineage.client.OpenLineage;
import java.lang.reflect.Field;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.WeakHashMap;
import lombok.extern.slf4j.Slf4j;
import openlineage.spark.agent.client.OpenLineageClient;
import openlineage.spark.agent.lifecycle.ContextFactory;
import openlineage.spark.agent.lifecycle.ExecutionContext;
import openlineage.spark.agent.lifecycle.SparkSQLExecutionContext;
import openlineage.spark.agent.transformers.PairRDDFunctionsTransformer;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.SparkContext$;
import org.apache.spark.SparkEnv;
import org.apache.spark.SparkEnv$;
import org.apache.spark.rdd.PairRDDFunctions;
import org.apache.spark.rdd.RDD;
import org.apache.spark.scheduler.SparkListenerApplicationStart;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.scheduler.SparkListenerJobEnd;
import org.apache.spark.scheduler.SparkListenerJobStart;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart;

@Slf4j
public class OpenLineageSparkListener extends org.apache.spark.scheduler.SparkListener {

  private static final Map<Long, SparkSQLExecutionContext> sparkSqlExecutionRegistry =
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
  private static WeakHashMap<RDD<?>, Configuration> outputs = new WeakHashMap<>();
  private static ContextFactory contextFactory;

  /** called by the agent on init with the provided argument */
  public static void init(ContextFactory contextFactory) {
    OpenLineageSparkListener.contextFactory = contextFactory;
    clear();
  }

  /**
   * Entrypoint for SparkSQLExecutionContext
   *
   * <p>called through the agent when creating the Spark context We register a new SparkListener
   *
   * @param context the spark context
   */
  @SuppressWarnings("unused")
  public static void instrument(SparkContext context) {
    log.info("Initializing OpenLineage SparkContext listener...");
    OpenLineageSparkListener listener = new OpenLineageSparkListener();
    log.debug(
        "Initialized OpenLineage listener with \nspark version: {}\njava.version: {}\nconfiguration: {}",
        context.version(),
        System.getProperty("java.version"),
        context.conf());
    context.addSparkListener(listener);
  }

  /**
   * Entry point for PairRDDFunctionsTransformer
   *
   * <p>called through the agent when writing with the RDD API as the RDDs do not contain the output
   * information
   *
   * @see PairRDDFunctionsTransformer
   * @param pairRDDFunctions the wrapping RDD containing the rdd to save
   * @param conf the write config
   */
  @SuppressWarnings("unused")
  public static void registerOutput(PairRDDFunctions<?, ?> pairRDDFunctions, Configuration conf) {
    try {
      log.info("Initializing OpenLineage PairRDDFunctions listener...");
      Field[] declaredFields = pairRDDFunctions.getClass().getDeclaredFields();
      for (Field field : declaredFields) {
        if (field.getName().endsWith("self") && RDD.class.isAssignableFrom(field.getType())) {
          field.setAccessible(true);
          try {
            RDD<?> rdd = (RDD<?>) field.get(pairRDDFunctions);
            outputs.put(rdd, conf);
          } catch (IllegalArgumentException | IllegalAccessException e) {
            e.printStackTrace(System.out);
          }
        }
      }
    } catch (Exception e) {
      log.error("Could not initialize OpenLineage PairRDDFunctions listener", e);
      emitError(e);
    }
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
    SparkSQLExecutionContext context = getSparkSQLExecutionContext(startEvent.executionId());
    context.start(startEvent);
  }

  /** called by the SparkListener when a spark-sql (Dataset api) execution ends */
  private static void sparkSQLExecEnd(SparkListenerSQLExecutionEnd endEvent) {
    SparkSQLExecutionContext context = sparkSqlExecutionRegistry.remove(endEvent.executionId());
    if (context != null) {
      context.end(endEvent);
    }
  }

  /** called by the SparkListener when a job starts */
  @Override
  public void onJobStart(SparkListenerJobStart jobStart) {
    asJavaOptional(
            SparkSession.getActiveSession()
                .map(toScalaFn(sess -> sess.sparkContext()))
                .orElse(toScalaFn(() -> SparkContext$.MODULE$.getActive())))
        .flatMap(ctx -> asJavaOptional(ctx.dagScheduler().jobIdToActiveJob().get(jobStart.jobId())))
        .ifPresent(
            job -> {
              String executionIdProp = job.properties().getProperty("spark.sql.execution.id");
              ExecutionContext context;
              if (executionIdProp != null) {
                long executionId = Long.parseLong(executionIdProp);
                context = getExecutionContext(job.jobId(), executionId);
              } else {
                context = getExecutionContext(job.jobId());
              }
              context.setActiveJob(job);
              context.start(jobStart);
            });
  }

  /** called by the SparkListener when a job ends */
  @Override
  public void onJobEnd(SparkListenerJobEnd jobEnd) {
    ExecutionContext context = rddExecutionRegistry.remove(jobEnd.jobId());
    context.end(jobEnd);
  }

  public static SparkSQLExecutionContext getSparkSQLExecutionContext(long executionId) {
    return sparkSqlExecutionRegistry.computeIfAbsent(
        executionId, (e) -> contextFactory.createSparkSQLExecutionContext(executionId));
  }

  public static ExecutionContext getExecutionContext(int jobId) {
    return rddExecutionRegistry.computeIfAbsent(
        jobId, (e) -> contextFactory.createRddExecutionContext(jobId));
  }

  public static ExecutionContext getExecutionContext(int jobId, long executionId) {
    ExecutionContext executionContext = getSparkSQLExecutionContext(executionId);
    rddExecutionRegistry.put(jobId, executionContext);
    return executionContext;
  }

  public static Configuration getConfigForRDD(RDD<?> rdd) {
    return outputs.get(rdd);
  }

  public static void emitError(Exception e) {
    OpenLineage ol = new OpenLineage(URI.create(OpenLineageClient.OPEN_LINEAGE_CLIENT_URI));
    try {
      contextFactory.sparkContext.emit(buildErrorLineageEvent(ol, errorRunFacet(e, ol)));
    } catch (Exception ex) {
      log.error("Could not emit open lineage on error", e);
    }
  }

  private static OpenLineage.RunFacets errorRunFacet(Exception e, OpenLineage ol) {
    OpenLineage.CustomFacetBuilder errorFacet = ol.newCustomFacetBuilder();
    errorFacet.put("exception", e);

    OpenLineage.RunFacetsBuilder runFacetsBuilder = ol.newRunFacetsBuilder();
    runFacetsBuilder.put("lineage.error", errorFacet.build());
    return runFacetsBuilder.build();
  }

  public static OpenLineage.RunEvent buildErrorLineageEvent(
      OpenLineage ol, OpenLineage.RunFacets runFacets) {
    return ol.newRunEventBuilder()
        .eventTime(ZonedDateTime.now())
        // TODO: what UUID should be here, cause we don't have ExecutionContext here?
        .run(ol.newRun(UUID.randomUUID(), runFacets))
        .job(
            ol.newJobBuilder()
                .namespace(contextFactory.sparkContext.getJobNamespace())
                .name(contextFactory.sparkContext.getParentJobName())
                .build())
        .build();
  }

  private static void clear() {
    sparkSqlExecutionRegistry.clear();
    rddExecutionRegistry.clear();
    outputs.clear();
  }

  /** To close the underlying resources. */
  public static void close() {
    clear();
    OpenLineageSparkListener.contextFactory.close();
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
        contextFactory = new ContextFactory(new OpenLineageContext(args));
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
      return new ArgumentParser(host, version, namespace, jobName, runId, apiKey);
    }
  }

  private String findSparkConfigKey(SparkConf conf, String name, String defaultValue) {
    return findSparkConfigKey(conf, name).orElse(defaultValue);
  }

  private Optional<String> findSparkConfigKey(SparkConf conf, String name) {
    return asJavaOptional(
        conf.getOption(name).getOrElse(toScalaFn(() -> conf.getOption("spark." + name))));
  }
}
