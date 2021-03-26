package marquez.spark.agent;

import java.lang.reflect.Field;
import java.lang.reflect.Proxy;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.WeakHashMap;
import lombok.extern.slf4j.Slf4j;
import marquez.spark.agent.client.LineageEvent;
import marquez.spark.agent.client.LineageEvent.Job;
import marquez.spark.agent.client.LineageEvent.Run;
import marquez.spark.agent.client.LineageEvent.RunFacet;
import marquez.spark.agent.client.OpenLineageClient;
import marquez.spark.agent.facets.ErrorFacet;
import marquez.spark.agent.lifecycle.ContextFactory;
import marquez.spark.agent.lifecycle.ExecutionContext;
import marquez.spark.agent.lifecycle.SparkSQLExecutionContext;
import marquez.spark.agent.transformers.ActiveJobTransformer;
import marquez.spark.agent.transformers.PairRDDFunctionsTransformer;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkContext;
import org.apache.spark.rdd.PairRDDFunctions;
import org.apache.spark.rdd.RDD;
import org.apache.spark.scheduler.ActiveJob;
import org.apache.spark.scheduler.SparkListenerInterface;
import org.apache.spark.scheduler.SparkListenerJobEnd;
import org.apache.spark.scheduler.SparkListenerJobStart;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart;

@Slf4j
public class SparkListener {
  private static final Map<Long, SparkSQLExecutionContext> sparkSqlExecutionRegistry =
      Collections.synchronizedMap(new HashMap<>());
  private static final Map<Integer, ExecutionContext> rddExecutionRegistry =
      Collections.synchronizedMap(new HashMap<>());
  private static WeakHashMap<RDD<?>, Configuration> outputs = new WeakHashMap<>();
  private static ContextFactory contextFactory;

  /** called by the agent on init with the provided argument */
  public static void init(ContextFactory contextFactory) {
    SparkListener.contextFactory = contextFactory;
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
    Class<?>[] interfaces = {SparkListenerInterface.class};
    SparkListenerInterface listener =
        (SparkListenerInterface)
            Proxy.newProxyInstance(
                SparkListener.class.getClassLoader(),
                interfaces,
                (proxy, method, args) -> {
                  try {
                    if (args.length == 1 && args[0] != null) {
                      Object arg = args[0];
                      if (method.getName().equals("onJobStart")
                          && arg instanceof SparkListenerJobStart) {
                        // rdd execution context
                        jobStarted((SparkListenerJobStart) args[0]);
                      } else if (method.getName().equals("onJobEnd")
                          && arg instanceof SparkListenerJobEnd) {
                        jobEnded((SparkListenerJobEnd) arg);
                      } else if (method.getName().equals("onOtherEvent")
                          && arg instanceof SparkListenerSQLExecutionStart) {
                        // spark sql
                        sparkSQLExecStart((SparkListenerSQLExecutionStart) arg);
                      } else if (method.getName().equals("onOtherEvent")
                          && arg instanceof SparkListenerSQLExecutionEnd) {
                        sparkSQLExecEnd((SparkListenerSQLExecutionEnd) arg);
                      }
                    }
                  } catch (Exception e) {
                    log.error("Could not run OpenLineage SparkContext listener proxy function", e);
                    emitError(e);
                  }
                  return null;
                });
    log.debug(
        "Initialized OpenLineage listener with \nspark version: {}\njava.version: {}\nconfiguration: {}",
        context.version(),
        System.getProperty("java.version"),
        context.conf());
    context.addSparkListener(listener);
  }

  /**
   * Entry point for ActiveJobTransformer
   *
   * <p>Called through the agent to register every new job and get access to the RDDs
   *
   * @see ActiveJobTransformer
   */
  @SuppressWarnings("unused")
  public static void registerActiveJob(ActiveJob activeJob) {
    try {
      log.info("Initializing OpenLineage ActiveJob listener...");
      String executionIdProp = activeJob.properties().getProperty("spark.sql.execution.id");
      ExecutionContext context;
      if (executionIdProp != null) {
        long executionId = Long.parseLong(executionIdProp);
        context = getExecutionContext(activeJob.jobId(), executionId);
      } else {
        context = getExecutionContext(activeJob.jobId());
      }
      context.setActiveJob(activeJob);
    } catch (Exception e) {
      log.error("Could not initialize OpenLineage ActiveJob listener", e);
      emitError(e);
    }
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
  private static void jobStarted(SparkListenerJobStart jobStart) {
    ExecutionContext context = getExecutionContext(jobStart.jobId());
    context.start(jobStart);
  }

  /** called by the SparkListener when a job ends */
  private static void jobEnded(SparkListenerJobEnd jobEnd) {
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
    try {
      contextFactory.marquezContext.emit(buildErrorLineageEvent(buildRunFacet(buildErrorFacet(e))));
    } catch (Exception ex) {
      log.error("Could not emit open lineage on error", e);
    }
  }

  public static LineageEvent buildErrorLineageEvent(RunFacet runFacet) {
    return LineageEvent.builder()
        .eventTime(ZonedDateTime.now())
        .run(
            Run.builder()
                .runId(contextFactory.marquezContext.getParentRunId())
                .facets(runFacet)
                .build())
        .job(
            Job.builder()
                .name(contextFactory.marquezContext.getJobName())
                .namespace(contextFactory.marquezContext.getJobNamespace())
                .build())
        .producer(OpenLineageClient.OPEN_LINEAGE_CLIENT_URI)
        .build();
  }

  public static RunFacet buildRunFacet(ErrorFacet errorFacet) {
    Map<String, Object> facets = new HashMap<>();
    facets.put("lineage.error", errorFacet);

    return RunFacet.builder().additional(facets).build();
  }

  public static ErrorFacet buildErrorFacet(Exception e) {
    return ErrorFacet.builder().exception(e).build();
  }

  private static void clear() {
    sparkSqlExecutionRegistry.clear();
    rddExecutionRegistry.clear();
    outputs.clear();
  }

  /** To close the underlying resources. */
  public static void close() {
    clear();
    SparkListener.contextFactory.close();
  }
}
