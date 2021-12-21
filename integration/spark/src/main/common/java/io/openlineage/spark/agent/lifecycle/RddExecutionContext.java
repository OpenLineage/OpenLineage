package io.openlineage.spark.agent.lifecycle;

import static scala.collection.JavaConversions.asJavaCollection;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.EventEmitter;
import io.openlineage.spark.agent.OpenLineageSparkListener;
import io.openlineage.spark.agent.client.DatasetParser;
import io.openlineage.spark.agent.client.DatasetParser.DatasetParseResult;
import io.openlineage.spark.agent.client.OpenLineageClient;
import io.openlineage.spark.agent.facets.ErrorFacet;
import io.openlineage.spark.agent.facets.SparkVersionFacet;
import io.openlineage.spark.agent.util.PlanUtils;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.api.OpenLineageContext;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URI;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.parquet.Strings;
import org.apache.spark.Dependency;
import org.apache.spark.SparkContext;
import org.apache.spark.SparkContext$;
import org.apache.spark.TaskContext;
import org.apache.spark.internal.io.HadoopMapRedWriteConfigUtil;
import org.apache.spark.rdd.HadoopRDD;
import org.apache.spark.rdd.MapPartitionsRDD;
import org.apache.spark.rdd.NewHadoopRDD;
import org.apache.spark.rdd.RDD;
import org.apache.spark.scheduler.ActiveJob;
import org.apache.spark.scheduler.JobFailed;
import org.apache.spark.scheduler.JobResult;
import org.apache.spark.scheduler.ResultStage;
import org.apache.spark.scheduler.SparkListenerJobEnd;
import org.apache.spark.scheduler.SparkListenerJobStart;
import org.apache.spark.scheduler.Stage;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart;
import org.apache.spark.util.SerializableJobConf;
import scala.Function2;
import scala.collection.Iterator;
import scala.collection.Seq;
import scala.runtime.AbstractFunction0;

@Slf4j
class RddExecutionContext implements ExecutionContext {
  private final EventEmitter sparkContext;
  private final Optional<SparkContext> sparkContextOption;
  private final UUID runId = UUID.randomUUID();
  private List<URI> inputs;
  private List<URI> outputs;
  private String jobSuffix;

  public RddExecutionContext(OpenLineageContext context, int jobId, EventEmitter sparkContext) {
    this.sparkContext = sparkContext;
    sparkContextOption =
        Optional.ofNullable(
            SparkContext$.MODULE$
                .getActive()
                .getOrElse(
                    new AbstractFunction0<SparkContext>() {
                      @Override
                      public SparkContext apply() {
                        return null;
                      }
                    }));
  }

  @Override
  public void setActiveJob(ActiveJob activeJob) {
    RDD<?> finalRDD = activeJob.finalStage().rdd();
    this.jobSuffix = nameRDD(finalRDD);
    Set<RDD<?>> rdds = Rdds.flattenRDDs(finalRDD);
    this.inputs = findInputs(rdds);
    Configuration jc = new JobConf();
    if (activeJob.finalStage() instanceof ResultStage) {
      Function2<TaskContext, Iterator<?>, ?> fn = ((ResultStage) activeJob.finalStage()).func();
      try {
        Field f = getConfigField(fn);
        f.setAccessible(true);

        HadoopMapRedWriteConfigUtil configUtil =
            Optional.of(f.get(fn))
                .filter(HadoopMapRedWriteConfigUtil.class::isInstance)
                .map(HadoopMapRedWriteConfigUtil.class::cast)
                .orElseThrow(
                    () ->
                        new NoSuchFieldException(
                            "Field is not instance of HadoopMapRedWriteConfigUtil"));

        Field confField = HadoopMapRedWriteConfigUtil.class.getDeclaredField("conf");
        confField.setAccessible(true);
        SerializableJobConf conf = (SerializableJobConf) confField.get(configUtil);
        jc = conf.value();
      } catch (IllegalAccessException | NoSuchFieldException nfe) {
        log.warn("Unable to access job conf from RDD", nfe);
      }
      log.info("Found job conf from RDD {}", jc);
    } else {
      jc = OpenLineageSparkListener.getConfigForRDD(finalRDD);
    }
    this.outputs = findOutputs(finalRDD, jc);
  }

  /**
   * Retrieves HadoopMapRedWriteConfigUtil field from function.<br>
   * In spark2 we can get it by "config$1" field.<br>
   * In spark3 we can get it by "arg$1" field
   *
   * @param fn
   * @return HadoopMapRedWriteConfigUtil field
   * @throws NoSuchFieldException
   */
  private Field getConfigField(Function2<TaskContext, Iterator<?>, ?> fn)
      throws NoSuchFieldException {
    try {
      return fn.getClass().getDeclaredField("config$1");
    } catch (NoSuchFieldException e) {
      return fn.getClass().getDeclaredField("arg$1");
    }
  }

  static String nameRDD(RDD<?> rdd) {
    String rddName = (String) rdd.name();
    if (rddName == null

        // HadoopRDDs are always named for the path. Don't name the RDD for a file. Otherwise, the
        // job name will end up differing each time we read a path with a date or other variable
        // directory name
        || (rdd instanceof HadoopRDD
            && Arrays.stream(FileInputFormat.getInputPaths(((HadoopRDD) rdd).getJobConf()))
                .anyMatch(p -> p.toString().contains(rdd.name())))
        // If the map RDD is named the same as its dependent, just use map_partition
        // This happens, e.g., when calling sparkContext.textFile(), as it creates a HadoopRDD, maps
        // the value to a string, and sets the name of the mapped RDD to the path, which is already
        // the name of the underlying HadoopRDD
        || (rdd instanceof MapPartitionsRDD
            && rdd.name().equals(((MapPartitionsRDD) rdd).prev().name()))) {
      rddName =
          rdd.getClass()
              .getSimpleName()
              .replaceAll("RDD\\d*$", "") // remove the trailing RDD from the class name
              .replaceAll(CAMEL_TO_SNAKE_CASE, "_$1") // camel case to snake case
              .toLowerCase(Locale.ROOT);
    }
    Seq<Dependency<?>> deps = (Seq<Dependency<?>>) rdd.dependencies();
    List<Dependency<?>> dependencies = ScalaConversionUtils.fromSeq(deps);
    if (dependencies.isEmpty()) {
      return rddName;
    }
    List<String> dependencyNames = new ArrayList<>();
    for (Dependency d : dependencies) {
      dependencyNames.add(nameRDD(d.rdd()));
    }
    String dependencyName = Strings.join(dependencyNames, "_");
    if (!dependencyName.startsWith(rddName)) {
      return rddName + "_" + dependencyName;
    } else {
      return dependencyName;
    }
  }

  @Override
  public void start(SparkListenerSQLExecutionStart sqlStart) {
    // do nothing
  }

  @Override
  public void end(SparkListenerSQLExecutionEnd sqlEnd) {
    // do nothing
  }

  @Override
  public void start(SparkListenerJobStart jobStart) {
    OpenLineage ol = new OpenLineage(OpenLineageClient.OPEN_LINEAGE_CLIENT_URI);
    OpenLineage.RunEvent event =
        ol.newRunEventBuilder()
            .eventTime(toZonedTime(jobStart.time()))
            .eventType("START")
            .inputs(buildInputs(inputs))
            .outputs(buildOutputs(outputs))
            .run(ol.newRunBuilder().runId(runId).facets(buildRunFacets(null)).build())
            .job(buildJob(jobStart.jobId()))
            .build();

    log.debug("Posting event for start {}: {}", jobStart, event);
    sparkContext.emit(event);
  }

  @Override
  public void end(SparkListenerJobEnd jobEnd) {
    OpenLineage ol = new OpenLineage(OpenLineageClient.OPEN_LINEAGE_CLIENT_URI);
    OpenLineage.RunEvent event =
        ol.newRunEventBuilder()
            .eventTime(toZonedTime(jobEnd.time()))
            .eventType(getEventType(jobEnd.jobResult()))
            .inputs(buildInputs(inputs))
            .outputs(buildOutputs(outputs))
            .run(
                ol.newRunBuilder()
                    .runId(runId)
                    .facets(buildRunFacets(buildJobErrorFacet(jobEnd.jobResult())))
                    .build())
            .job(buildJob(jobEnd.jobId()))
            .build();

    log.debug("Posting event for end {}: {}", jobEnd, event);
    sparkContext.emit(event);
  }

  protected ZonedDateTime toZonedTime(long time) {
    Instant i = Instant.ofEpochMilli(time);
    return ZonedDateTime.ofInstant(i, ZoneOffset.UTC);
  }

  protected OpenLineage.RunFacets buildRunFacets(ErrorFacet jobError) {
    OpenLineage.RunFacetsBuilder builder = new OpenLineage.RunFacetsBuilder();
    buildParentFacet().ifPresent(builder::parent);
    if (jobError != null) {
      builder.put("spark.exception", jobError);
    }
    builder.put("spark_version", new SparkVersionFacet(SparkSession.active()));
    return builder.build();
  }

  private Optional<OpenLineage.ParentRunFacet> buildParentFacet() {
    return sparkContext
        .getParentRunId()
        .map(
            runId ->
                PlanUtils.parentRunFacet(
                    runId, sparkContext.getParentJobName(), sparkContext.getJobNamespace()));
  }

  protected ErrorFacet buildJobErrorFacet(JobResult jobResult) {
    if (jobResult instanceof JobFailed && ((JobFailed) jobResult).exception() != null) {
      return ErrorFacet.builder().exception(((JobFailed) jobResult).exception()).build();
    }
    return null;
  }

  protected OpenLineage.Job buildJob(int jobId) {
    String suffix = jobSuffix;
    if (jobSuffix == null) {
      suffix = String.valueOf(jobId);
    }
    String jobName = sparkContextOption.map(SparkContext::appName).orElse("unknown") + "." + suffix;
    return new OpenLineage.JobBuilder()
        .namespace(sparkContext.getJobNamespace())
        .name(jobName.replaceAll(CAMEL_TO_SNAKE_CASE, "_$1").toLowerCase(Locale.ROOT))
        .build();
  }

  protected List<OpenLineage.OutputDataset> buildOutputs(List<URI> outputs) {
    return outputs.stream().map(this::buildOutputDataset).collect(Collectors.toList());
  }

  protected OpenLineage.InputDataset buildInputDataset(URI uri) {
    DatasetParseResult result = DatasetParser.parse(uri);
    return new OpenLineage.InputDatasetBuilder()
        .name(result.getName())
        .namespace(result.getNamespace())
        .build();
  }

  protected OpenLineage.OutputDataset buildOutputDataset(URI uri) {
    DatasetParseResult result = DatasetParser.parse(uri);
    return new OpenLineage.OutputDatasetBuilder()
        .name(result.getName())
        .namespace(result.getNamespace())
        .build();
  }

  protected List<OpenLineage.InputDataset> buildInputs(List<URI> inputs) {
    return inputs.stream().map(this::buildInputDataset).collect(Collectors.toList());
  }

  protected List<URI> findOutputs(RDD<?> rdd, Configuration config) {
    Path outputPath = getOutputPath(rdd, config);
    log.info("Found output path {} from RDD {}", outputPath, rdd);
    if (outputPath != null) {
      return Collections.singletonList(getDatasetUri(outputPath.toUri()));
    }
    return Collections.emptyList();
  }

  protected List<URI> findInputs(Set<RDD<?>> rdds) {
    List<URI> result = new ArrayList<>();
    for (RDD<?> rdd : rdds) {
      Path[] inputPaths = getInputPaths(rdd);
      if (inputPaths != null) {
        for (Path path : inputPaths) {
          result.add(getDatasetUri(path.toUri()));
        }
      }
    }
    return result;
  }

  protected Path[] getInputPaths(RDD<?> rdd) {
    Path[] inputPaths = null;
    if (rdd instanceof HadoopRDD) {
      inputPaths =
          org.apache.hadoop.mapred.FileInputFormat.getInputPaths(
              ((HadoopRDD<?, ?>) rdd).getJobConf());
    } else if (rdd instanceof NewHadoopRDD) {
      try {
        inputPaths =
            org.apache.hadoop.mapreduce.lib.input.FileInputFormat.getInputPaths(
                new Job(((NewHadoopRDD<?, ?>) rdd).getConf()));
      } catch (IOException e) {
        log.error("Openlineage spark agent could not get input paths", e);
      }
    }
    return inputPaths;
  }

  // exposed for testing
  protected URI getDatasetUri(URI pathUri) {
    return pathUri;
  }

  protected void printRDDs(String prefix, RDD<?> rdd) {
    Collection<Dependency<?>> deps = asJavaCollection(rdd.dependencies());
    for (Dependency<?> dep : deps) {
      printRDDs(prefix + "  ", dep.rdd());
    }
  }

  private void printStages(String prefix, Stage stage) {
    if (stage instanceof ResultStage) {
      ResultStage resultStage = (ResultStage) stage;
    }
    printRDDs(
        prefix + "(stageId:" + stage.id() + ")-(" + stage.getClass().getSimpleName() + ")- RDD: ",
        stage.rdd());
    Collection<Stage> parents = asJavaCollection(stage.parents());
    for (Stage parent : parents) {
      printStages(prefix + " \\ ", parent);
    }
  }

  protected static Path getOutputPath(RDD<?> rdd, Configuration config) {
    if (config == null) {
      return null;
    }
    // "new" mapred api
    JobConf jc;
    if (config instanceof JobConf) {
      jc = (JobConf) config;
    } else {
      jc = new JobConf(config);
    }
    Path path = org.apache.hadoop.mapred.FileOutputFormat.getOutputPath(jc);
    if (path == null) {
      try {
        // old fashioned mapreduce api
        path = org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.getOutputPath(new Job(jc));
      } catch (IOException exception) {
        exception.printStackTrace(System.out);
      }
    }
    return path;
  }

  protected String getEventType(JobResult jobResult) {
    if (jobResult.getClass().getSimpleName().startsWith("JobSucceeded")) {
      return "COMPLETE";
    }
    return "FAIL";
  }
}
