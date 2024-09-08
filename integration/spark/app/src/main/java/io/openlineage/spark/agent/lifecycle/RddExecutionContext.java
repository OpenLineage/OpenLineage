/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle;

import static io.openlineage.spark.agent.util.TimeUtils.toZonedTime;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.client.utils.UUIDUtils;
import io.openlineage.spark.agent.EventEmitter;
import io.openlineage.spark.agent.OpenLineageSparkListener;
import io.openlineage.spark.agent.facets.ErrorFacet;
import io.openlineage.spark.agent.facets.builder.GcpJobFacetBuilder;
import io.openlineage.spark.agent.facets.builder.GcpRunFacetBuilder;
import io.openlineage.spark.agent.facets.builder.SparkJobDetailsFacetBuilder;
import io.openlineage.spark.agent.facets.builder.SparkProcessingEngineRunFacetBuilderDelegate;
import io.openlineage.spark.agent.facets.builder.SparkPropertyFacetBuilder;
import io.openlineage.spark.agent.util.GCPUtils;
import io.openlineage.spark.agent.util.PathUtils;
import io.openlineage.spark.agent.util.PlanUtils;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.agent.util.StreamingContextUtils;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.naming.JobNameBuilder;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
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
import org.apache.spark.internal.io.HadoopMapRedWriteConfigUtil;
import org.apache.spark.internal.io.HadoopMapReduceWriteConfigUtil;
import org.apache.spark.rdd.HadoopRDD;
import org.apache.spark.rdd.MapPartitionsRDD;
import org.apache.spark.rdd.RDD;
import org.apache.spark.scheduler.*;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart;
import org.apache.spark.util.SerializableJobConf;
import scala.collection.Seq;

@Slf4j
class RddExecutionContext implements ExecutionContext {
  private static final String SPARK_PROCESSING_TYPE_BATCH = "BATCH";
  private static final String SPARK_PROCESSING_TYPE_STREAMING = "STREAMING";
  private static final String SPARK_JOB_TYPE = "RDD_JOB";

  private final EventEmitter eventEmitter;
  private final UUID runId = UUIDUtils.generateNewUUID();
  private final OpenLineageContext olContext;
  private List<URI> inputs = Collections.emptyList();
  private List<URI> outputs = Collections.emptyList();
  private String jobSuffix;

  public RddExecutionContext(OpenLineageContext olContext, EventEmitter eventEmitter) {
    this.eventEmitter = eventEmitter;
    this.olContext = olContext;
  }

  @Override
  public void start(SparkListenerStageSubmitted stageSubmitted) {}

  @Override
  public void end(SparkListenerStageCompleted stageCompleted) {}

  @Override
  public void start(SparkListenerApplicationStart applicationStart) {}

  @Override
  public void end(SparkListenerApplicationEnd applicationEnd) {}

  @Override
  @SuppressWarnings("PMD") // f.setAccessible(true);
  public void setActiveJob(ActiveJob activeJob) {
    log.debug("setActiveJob within RddExecutionContext {}", activeJob);
    RDD<?> finalRDD = activeJob.finalStage().rdd();
    this.jobSuffix = nameRDD(finalRDD);
    Set<RDD<?>> rdds = Rdds.flattenRDDs(finalRDD);
    log.debug("flattenRDDs {}", rdds);
    this.inputs = findInputs(rdds);
    Configuration jc = new JobConf();
    if (activeJob.finalStage() instanceof ResultStage) {
      ResultStage resultStage = (ResultStage) activeJob.finalStage();
      try {
        Field f = getConfigField(resultStage);
        f.setAccessible(true);
        Object conf = f.get(resultStage.func());

        if (conf instanceof HadoopMapRedWriteConfigUtil) {
          Field confField = HadoopMapRedWriteConfigUtil.class.getDeclaredField("conf");
          confField.setAccessible(true);
          SerializableJobConf serializableJobConf = (SerializableJobConf) confField.get(conf);
          jc = serializableJobConf.value();
        } else if (conf instanceof HadoopMapReduceWriteConfigUtil) {
          Field confField = HadoopMapReduceWriteConfigUtil.class.getDeclaredField("conf");
          confField.setAccessible(true);
          SerializableJobConf serializableJobConf = (SerializableJobConf) confField.get(conf);
          jc = serializableJobConf.value();
        } else {
          log.info(
              "Config field is not HadoopMapRedWriteConfigUtil or HadoopMapReduceWriteConfigUtil, it's {}",
              conf.getClass().getCanonicalName());
        }
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
   * @param resultStage
   * @return HadoopMapRedWriteConfigUtil field
   * @throws NoSuchFieldException
   */
  private Field getConfigField(ResultStage resultStage) throws NoSuchFieldException {
    try {
      return resultStage.func().getClass().getDeclaredField("config$1");
    } catch (NoSuchFieldException e) {
      return resultStage.func().getClass().getDeclaredField("arg$1");
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
    log.debug("start SparkListenerSQLExecutionStart {}", sqlStart);
  }

  @Override
  public void end(SparkListenerSQLExecutionEnd sqlEnd) {
    // do nothing
    log.debug("start SparkListenerSQLExecutionEnd {}", sqlEnd);
  }

  @Override
  public void start(SparkListenerJobStart jobStart) {
    log.debug("start SparkListenerJobStart {}", jobStart);
    if (outputs.isEmpty()) {
      // Oftentimes SparkListener is triggered for actions which do not contain any meaningful
      // lineage data and are useless in the context of lineage graph. We assume this occurs
      // for RDD operations which have no output dataset
      log.info("Output RDDs are empty: skipping sending OpenLineage event");
      return;
    }
    OpenLineage.RunEvent event =
        olContext
            .getOpenLineage()
            .newRunEventBuilder()
            .eventTime(toZonedTime(jobStart.time()))
            .eventType(OpenLineage.RunEvent.EventType.START)
            .inputs(buildInputs(inputs))
            .outputs(buildOutputs(outputs))
            .run(
                olContext
                    .getOpenLineage()
                    .newRunBuilder()
                    .runId(runId)
                    .facets(buildRunFacets(null, jobStart))
                    .build())
            .job(buildJob(jobStart.jobId()))
            .build();
    log.debug("Posting event for start {}: {}", jobStart, event);
    eventEmitter.emit(event);
  }

  @Override
  public void end(SparkListenerJobEnd jobEnd) {
    log.debug("end SparkListenerJobEnd {}", jobEnd);
    if (outputs.isEmpty() && !(jobEnd.jobResult() instanceof JobFailed)) {
      // Oftentimes SparkListener is triggered for actions which do not contain any
      // meaningful
      // lineage data and are useless in the context of lineage graph. We assume this
      // occurs
      // for RDD operations which have no output dataset
      log.info("Output RDDs are empty: skipping sending OpenLineage event");
      return;
    }
    OpenLineage.RunEvent event =
        olContext
            .getOpenLineage()
            .newRunEventBuilder()
            .eventTime(toZonedTime(jobEnd.time()))
            .eventType(getEventType(jobEnd.jobResult()))
            .inputs(buildInputs(inputs))
            .outputs(buildOutputs(outputs))
            .run(
                olContext
                    .getOpenLineage()
                    .newRunBuilder()
                    .runId(runId)
                    .facets(buildRunFacets(buildJobErrorFacet(jobEnd.jobResult()), jobEnd))
                    .build())
            .job(buildJob(jobEnd.jobId()))
            .build();
    log.debug("Posting event for end {}: {}", jobEnd, event);
    eventEmitter.emit(event);
  }

  protected OpenLineage.RunFacets buildRunFacets(ErrorFacet jobError, SparkListenerEvent event) {
    OpenLineage.RunFacetsBuilder runFacetsBuilder =
        olContext.getOpenLineage().newRunFacetsBuilder();
    runFacetsBuilder.parent(buildApplicationParentFacet());
    if (jobError != null) {
      runFacetsBuilder.put("spark.exception", jobError);
    }

    addProcessingEventFacet(runFacetsBuilder);
    addSparkPropertyFacet(runFacetsBuilder, event);
    addGcpRunFacet(runFacetsBuilder, event);
    addSparkJobDetailsFacet(runFacetsBuilder, event);

    return runFacetsBuilder.build();
  }

  private void addProcessingEventFacet(OpenLineage.RunFacetsBuilder b0) {
    olContext
        .getSparkContext()
        .ifPresent(
            context -> {
              OpenLineage.ProcessingEngineRunFacet facet =
                  new SparkProcessingEngineRunFacetBuilderDelegate(
                          olContext.getOpenLineage(), context)
                      .buildFacet();
              b0.processing_engine(facet);
            });
  }

  private void addSparkPropertyFacet(OpenLineage.RunFacetsBuilder b0, SparkListenerEvent event) {
    b0.put("spark_properties", new SparkPropertyFacetBuilder().buildFacet(event));
  }

  private void addGcpRunFacet(OpenLineage.RunFacetsBuilder b0, SparkListenerEvent event) {
    if (!GCPUtils.isDataprocRuntime()) return;
    olContext
        .getSparkContext()
        .ifPresent(
            context -> {
              GcpRunFacetBuilder b1 = new GcpRunFacetBuilder(context);
              b1.accept(event, b0::put);
            });
  }

  private void addSparkJobDetailsFacet(OpenLineage.RunFacetsBuilder b0, SparkListenerEvent event) {
    b0.put("spark_jobDetails", new SparkJobDetailsFacetBuilder().buildFacet(event));
  }

  private OpenLineage.ParentRunFacet buildApplicationParentFacet() {
    return PlanUtils.parentRunFacet(
        eventEmitter.getApplicationRunId(),
        eventEmitter.getApplicationJobName(),
        eventEmitter.getJobNamespace());
  }

  protected OpenLineage.JobFacets buildJobFacets(SparkListenerEvent sparkListenerEvent) {
    OpenLineage.JobFacetsBuilder jobFacetsBuilder =
        olContext.getOpenLineage().newJobFacetsBuilder();
    addGcpJobFacets(jobFacetsBuilder, sparkListenerEvent);
    return jobFacetsBuilder.build();
  }

  private void addGcpJobFacets(OpenLineage.JobFacetsBuilder b0, SparkListenerEvent event) {
    if (!GCPUtils.isDataprocRuntime()) return;
    olContext
        .getSparkContext()
        .ifPresent(
            context -> {
              GcpJobFacetBuilder b1 = new GcpJobFacetBuilder(context);
              b1.accept(event, b0::put);
            });
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

    return olContext
        .getOpenLineage()
        .newJobBuilder()
        .namespace(eventEmitter.getJobNamespace())
        .name(JobNameBuilder.build(olContext, suffix))
        .facets(
            olContext
                .getOpenLineage()
                .newJobFacetsBuilder()
                .jobType(
                    olContext
                        .getOpenLineage()
                        .newJobTypeJobFacetBuilder()
                        .jobType(SPARK_JOB_TYPE)
                        .processingType(
                            StreamingContextUtils.hasActiveStreamingContext()
                                ? SPARK_PROCESSING_TYPE_STREAMING
                                : SPARK_PROCESSING_TYPE_BATCH)
                        .integration("SPARK")
                        .build())
                .build())
        .build();
  }

  protected List<OpenLineage.OutputDataset> buildOutputs(List<URI> outputs) {
    return outputs.stream().map(this::buildOutputDataset).collect(Collectors.toList());
  }

  protected OpenLineage.InputDataset buildInputDataset(URI uri) {
    DatasetIdentifier di = PathUtils.fromURI(uri);
    return olContext
        .getOpenLineage()
        .newInputDatasetBuilder()
        .name(di.getName())
        .namespace(di.getNamespace())
        .build();
  }

  protected OpenLineage.OutputDataset buildOutputDataset(URI uri) {
    DatasetIdentifier di = PathUtils.fromURI(uri);
    return olContext
        .getOpenLineage()
        .newOutputDatasetBuilder()
        .name(di.getName())
        .namespace(di.getNamespace())
        .build();
  }

  protected List<OpenLineage.InputDataset> buildInputs(List<URI> inputs) {
    return inputs.stream().map(this::buildInputDataset).collect(Collectors.toList());
  }

  protected List<URI> findOutputs(RDD<?> rdd, Configuration config) {
    Path outputPath = getOutputPath(rdd, config);
    log.info("Found output path {} from RDD {}", outputPath, rdd);
    if (outputPath != null) {
      return Collections.singletonList(outputPath.toUri());
    }
    log.debug("Output path is null");
    return Collections.emptyList();
  }

  protected List<URI> findInputs(Set<RDD<?>> rdds) {
    log.debug("find Inputs within RddExecutionContext {}", rdds);
    return PlanUtils.findRDDPaths(rdds.stream().collect(Collectors.toList())).stream()
        .map(path -> path.toUri())
        .collect(Collectors.toList());
  }

  protected void printRDDs(String prefix, RDD<?> rdd) {
    Collection<Dependency<?>> deps = ScalaConversionUtils.fromSeq(rdd.dependencies());
    for (Dependency<?> dep : deps) {
      printRDDs(prefix + "  ", dep.rdd());
    }
  }

  protected static Path getOutputPath(RDD<?> rdd, Configuration config) {
    Path path = null;
    if (config != null) {
      // "new" mapred api
      JobConf jc;
      if (config instanceof JobConf) {
        jc = (JobConf) config;
      } else {
        jc = new JobConf(config);
      }
      log.debug("JobConf {}", jc);
      path = org.apache.hadoop.mapred.FileOutputFormat.getOutputPath(jc);
      if (path == null) {
        try {
          // old fashioned mapreduce api
          log.debug("Path is null, trying to use old fashioned mapreduce api");
          path = org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.getOutputPath(new Job(jc));
        } catch (IOException exception) {
          exception.printStackTrace(System.out);
        }
      }
    }

    if (path == null) {
      // use PlanUtils approach instead
      path =
          PlanUtils.findRDDPaths(Collections.singletonList(rdd)).stream().findFirst().orElse(null);
    }

    return path;
  }

  protected OpenLineage.RunEvent.EventType getEventType(JobResult jobResult) {
    if (jobResult.getClass().getSimpleName().startsWith("JobSucceeded")) {
      return OpenLineage.RunEvent.EventType.COMPLETE;
    }
    return OpenLineage.RunEvent.EventType.FAIL;
  }
}
