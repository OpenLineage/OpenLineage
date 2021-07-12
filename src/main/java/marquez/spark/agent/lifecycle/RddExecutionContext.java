package marquez.spark.agent.lifecycle;

import static scala.collection.JavaConversions.asJavaCollection;

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
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import marquez.spark.agent.MarquezContext;
import marquez.spark.agent.SparkListener;
import marquez.spark.agent.client.DatasetParser;
import marquez.spark.agent.client.DatasetParser.DatasetParseResult;
import marquez.spark.agent.client.LineageEvent;
import marquez.spark.agent.client.LineageEvent.Dataset;
import marquez.spark.agent.client.LineageEvent.ParentRunFacet;
import marquez.spark.agent.client.LineageEvent.RunFacet;
import marquez.spark.agent.client.OpenLineageClient;
import marquez.spark.agent.facets.ErrorFacet;
import marquez.spark.agent.lifecycle.plan.PlanUtils;
import marquez.spark.agent.lifecycle.plan.ScalaConversionUtils;
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
import org.apache.spark.util.SerializableJobConf;
import scala.Function2;
import scala.collection.Iterator;
import scala.collection.Seq;
import scala.runtime.AbstractFunction0;

@Slf4j
public class RddExecutionContext implements ExecutionContext {
  private final MarquezContext marquezContext;
  private final Optional<SparkContext> sparkContextOption;
  private List<URI> inputs;
  private List<URI> outputs;
  private String jobSuffix;

  public RddExecutionContext(int jobId, MarquezContext marquezContext) {
    this.marquezContext = marquezContext;
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
        Field f = fn.getClass().getDeclaredField("config$1");
        f.setAccessible(true);

        HadoopMapRedWriteConfigUtil configUtil = (HadoopMapRedWriteConfigUtil) f.get(fn);
        Field confField = HadoopMapRedWriteConfigUtil.class.getDeclaredField("conf");
        confField.setAccessible(true);
        SerializableJobConf conf = (SerializableJobConf) confField.get(configUtil);
        jc = conf.value();
      } catch (IllegalAccessException | NoSuchFieldException nfe) {
        log.warn("Unable to access job conf from RDD", nfe);
      }
      log.info("Found job conf from RDD {}", jc);
    } else {
      jc = SparkListener.getConfigForRDD(finalRDD);
    }
    this.outputs = findOutputs(finalRDD, jc);
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
  public void start(SparkListenerJobStart jobStart) {
    LineageEvent event =
        LineageEvent.builder()
            .inputs(buildInputs(inputs))
            .outputs(buildOutputs(outputs))
            .run(buildRun(buildRunFacets(null)))
            .job(buildJob(jobStart.jobId()))
            .eventTime(toZonedTime(jobStart.time()))
            .eventType("START")
            .producer(OpenLineageClient.OPEN_LINEAGE_CLIENT_URI)
            .build();

    log.debug("Posting event for start {}: {}", jobStart, event);
    marquezContext.emit(event);
  }

  @Override
  public void end(SparkListenerJobEnd jobEnd) {
    LineageEvent event =
        LineageEvent.builder()
            .inputs(buildInputs(inputs))
            .outputs(buildOutputs(outputs))
            .run(buildRun(buildRunFacets(buildJobErrorFacet(jobEnd.jobResult()))))
            .job(buildJob(jobEnd.jobId()))
            .eventTime(toZonedTime(jobEnd.time()))
            .eventType(getEventType(jobEnd.jobResult()))
            .producer(OpenLineageClient.OPEN_LINEAGE_CLIENT_URI)
            .build();

    log.debug("Posting event for end {}: {}", jobEnd, event);
    marquezContext.emit(event);
  }

  protected ZonedDateTime toZonedTime(long time) {
    Instant i = Instant.ofEpochMilli(time);
    return ZonedDateTime.ofInstant(i, ZoneOffset.UTC);
  }

  protected LineageEvent.Run buildRun(RunFacet facets) {
    return LineageEvent.Run.builder().runId(marquezContext.getParentRunId()).facets(facets).build();
  }

  protected RunFacet buildRunFacets(ErrorFacet jobError) {
    Map<String, Object> additionalFacets = new HashMap<>();
    if (jobError != null) {
      additionalFacets.put("spark.exception", jobError);
    }
    return RunFacet.builder().parent(buildParentFacet()).additional(additionalFacets).build();
  }

  private ParentRunFacet buildParentFacet() {
    if (marquezContext.getParentRunId() != null
        && marquezContext.getParentRunId().trim().length() > 0) {
      return PlanUtils.parentRunFacet(
          marquezContext.getParentRunId(),
          marquezContext.getParentJobName(),
          marquezContext.getJobNamespace());
    } else {
      return null;
    }
  }

  protected ErrorFacet buildJobErrorFacet(JobResult jobResult) {
    if (jobResult instanceof JobFailed && ((JobFailed) jobResult).exception() != null) {
      return ErrorFacet.builder().exception(((JobFailed) jobResult).exception()).build();
    }
    return null;
  }

  protected LineageEvent.Job buildJob(int jobId) {
    String suffix = jobSuffix;
    if (jobSuffix == null) {
      suffix = String.valueOf(jobId);
    }
    String jobName = sparkContextOption.map(SparkContext::appName).orElse("unknown") + "." + suffix;
    return LineageEvent.Job.builder()
        .namespace(marquezContext.getJobNamespace())
        .name(jobName.replaceAll(CAMEL_TO_SNAKE_CASE, "_$1").toLowerCase(Locale.ROOT))
        .build();
  }

  protected List<Dataset> buildOutputs(List<URI> outputs) {
    return outputs.stream().map(this::buildDataset).collect(Collectors.toList());
  }

  protected Dataset buildDataset(URI uri) {
    DatasetParseResult result = DatasetParser.parse(uri);
    return Dataset.builder().name(result.getName()).namespace(result.getNamespace()).build();
  }

  protected List<Dataset> buildInputs(List<URI> inputs) {
    return inputs.stream().map(this::buildDataset).collect(Collectors.toList());
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
        log.error("Marquez spark agent could not get input paths", e);
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
