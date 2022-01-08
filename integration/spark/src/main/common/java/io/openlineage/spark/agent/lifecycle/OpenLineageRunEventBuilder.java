package io.openlineage.spark.agent.lifecycle;

import static io.openlineage.spark.agent.util.ScalaConversionUtils.toScalaFn;

import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.DatasetFacet;
import io.openlineage.client.OpenLineage.DatasetFacets;
import io.openlineage.client.OpenLineage.InputDataset;
import io.openlineage.client.OpenLineage.InputDatasetFacet;
import io.openlineage.client.OpenLineage.InputDatasetInputFacets;
import io.openlineage.client.OpenLineage.JobBuilder;
import io.openlineage.client.OpenLineage.JobFacet;
import io.openlineage.client.OpenLineage.OutputDataset;
import io.openlineage.client.OpenLineage.OutputDatasetFacet;
import io.openlineage.client.OpenLineage.OutputDatasetOutputFacets;
import io.openlineage.client.OpenLineage.ParentRunFacet;
import io.openlineage.client.OpenLineage.RunEvent;
import io.openlineage.client.OpenLineage.RunEventBuilder;
import io.openlineage.client.OpenLineage.RunFacet;
import io.openlineage.client.OpenLineage.RunFacets;
import io.openlineage.client.OpenLineage.RunFacetsBuilder;
import io.openlineage.spark.agent.JobMetricsHolder;
import io.openlineage.spark.agent.util.PlanUtils;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.api.CustomFacetBuilder;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.OpenLineageEventHandlerFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.AllArgsConstructor;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.rdd.RDD;
import org.apache.spark.scheduler.ActiveJob;
import org.apache.spark.scheduler.JobFailed;
import org.apache.spark.scheduler.SparkListenerJobEnd;
import org.apache.spark.scheduler.SparkListenerJobStart;
import org.apache.spark.scheduler.SparkListenerStageCompleted;
import org.apache.spark.scheduler.SparkListenerStageSubmitted;
import org.apache.spark.scheduler.Stage;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart;
import scala.PartialFunction;

/**
 * Event handler that accepts various {@link org.apache.spark.scheduler.SparkListener} events and
 * helps build up an {@link OpenLineage.RunEvent} by passing event components to partial functions
 * that know how to convert those event components into {@link OpenLineage.RunEvent} properties.
 *
 * <p>The event types that can be consumed to generate @link OpenLineage.RunEvent} properties have
 * no common supertype, so the generic argument for the function input is simply {@link Object}. The
 * types of arguments that may be found include
 *
 * <ul>
 *   <li>{@link org.apache.spark.scheduler.StageInfo}
 *   <li>{@link org.apache.spark.scheduler.Stage}
 *   <li>{@link org.apache.spark.rdd.RDD}
 *   <li>{@link org.apache.spark.scheduler.ActiveJob}
 *   <li>{@link org.apache.spark.sql.execution.QueryExecution}
 * </ul>
 *
 * <p>These components are extracted from various {@link org.apache.spark.scheduler.SparkListener}
 * events, such as {@link SparkListenerStageCompleted}, {@link SparkListenerJobStart}, and {@link
 * org.apache.spark.scheduler.SparkListenerTaskEnd}.
 *
 * <p>{@link RDD} chains will be _flattened_ so each `RDD` dependency is passed to the builders one
 * at a time. This means a builder can directly specify the type of {@link RDD} it handles, such as
 * a {@link org.apache.spark.rdd.HadoopRDD} or a {@link
 * org.apache.spark.sql.execution.datasources.FileScanRDD}, without having to check the dependencies
 * of every {@link org.apache.spark.rdd.MapPartitionsRDD} or {@link
 * org.apache.spark.sql.execution.SQLExecutionRDD}.
 *
 * <p>Any {@link RunFacet}s and {@link JobFacet}s returned by the {@link CustomFacetBuilder}s are
 * appended to the {@link io.openlineage.client.OpenLineage.Run} and {@link
 * io.openlineage.client.OpenLineage.Job}, respectively.
 *
 * <p><b>If</b> any {@link io.openlineage.client.OpenLineage.InputDatasetBuilder}s or {@link
 * io.openlineage.client.OpenLineage.OutputDatasetBuilder}s are returned from the partial functions,
 * the {@link #inputDatasetBuilders} or {@link #outputDatasetBuilders} will be invoked using the
 * same input arguments in order to construct any {@link InputDatasetFacet}s or {@link
 * OutputDatasetFacet}s to the returned dataset. {@link InputDatasetFacet}s and {@link
 * OutputDatasetFacet}s will be attached to <i>any</i> {@link
 * io.openlineage.client.OpenLineage.InputDatasetBuilder} or {@link
 * io.openlineage.client.OpenLineage.OutputDatasetBuilder} found for the event. This is because
 * facets may be constructed from generic information that is not specifically tied to a Dataset.
 * For example, {@link io.openlineage.client.OpenLineage.OutputStatisticsOutputDatasetFacet}s are
 * created from {@link org.apache.spark.executor.TaskMetrics} attached to the last {@link
 * org.apache.spark.scheduler.StageInfo} for a given job execution. However, the {@link
 * io.openlineage.client.OpenLineage.OutputDataset} is constructed by reading the {@link
 * org.apache.spark.sql.catalyst.plans.logical.LogicalPlan}. There's no way to tie the output
 * metrics in the {@link org.apache.spark.scheduler.StageInfo} to the {@link
 * io.openlineage.client.OpenLineage.OutputDataset} in the {@link
 * org.apache.spark.sql.catalyst.plans.logical.LogicalPlan} except by inference. Similarly, input
 * metrics can be found in the {@link org.apache.spark.scheduler.StageInfo} for the stage that reads
 * a dataset and the {@link io.openlineage.client.OpenLineage.InputDataset} can usually be
 * constructed by walking the {@link RDD} dependency tree for that {@link Stage} and finding a
 * {@link org.apache.spark.sql.execution.datasources.FileScanRDD} or other concrete implementation.
 * But while there is typically only one {@link io.openlineage.client.OpenLineage.InputDataset} read
 * in a given stage, there's no guarantee of that and the {@link
 * org.apache.spark.executor.TaskMetrics} in the {@link org.apache.spark.scheduler.StageInfo} won't
 * disambiguate.
 *
 * <p>If a facet needs to be attached to a specific dataset, the user must take care to construct
 * both the Dataset and the Facet in the same builder.
 */
@Slf4j
@AllArgsConstructor
class OpenLineageRunEventBuilder {

  @NonNull private final OpenLineageContext openLineageContext;

  @NonNull
  private final List<PartialFunction<Object, List<OpenLineage.InputDataset>>> inputDatasetBuilders;

  @NonNull
  private final List<PartialFunction<LogicalPlan, List<InputDataset>>>
      inputDatasetQueryPlanVisitors;

  @NonNull
  private final List<PartialFunction<Object, List<OpenLineage.OutputDataset>>>
      outputDatasetBuilders;

  @NonNull
  private final List<PartialFunction<LogicalPlan, List<OutputDataset>>>
      outputDatasetQueryPlanVisitors;

  @NonNull private final List<CustomFacetBuilder<?, ? extends DatasetFacet>> datasetFacetBuilders;

  @NonNull
  private final List<CustomFacetBuilder<?, ? extends InputDatasetFacet>> inputDatasetFacetBuilders;

  @NonNull
  private final List<CustomFacetBuilder<?, ? extends OutputDatasetFacet>>
      outputDatasetFacetBuilders;

  @NonNull private final List<CustomFacetBuilder<?, ? extends RunFacet>> runFacetBuilders;
  @NonNull private final List<CustomFacetBuilder<?, ? extends JobFacet>> jobFacetBuilders;

  private final UnknownEntryFacetListener<?> unknownEntryFacetListener =
      new UnknownEntryFacetListener();
  private final Map<Integer, ActiveJob> jobMap = new HashMap<>();
  private final Map<Integer, Stage> stageMap = new HashMap<>();

  private final ObjectMapper objectMapper = new ObjectMapper();
  private final JobMetricsHolder jobMetricsHolder = JobMetricsHolder.getInstance();

  OpenLineageRunEventBuilder(OpenLineageContext context, OpenLineageEventHandlerFactory factory) {
    this(
        context,
        factory.createInputDatasetBuilder(context),
        factory.createInputDatasetQueryPlanVisitors(context),
        factory.createOutputDatasetBuilder(context),
        factory.createOutputDatasetQueryPlanVisitors(context),
        factory.createDatasetFacetBuilders(context),
        factory.createInputDatasetFacetBuilders(context),
        factory.createOutputDatasetFacetBuilders(context),
        factory.createRunFacetBuilders(context),
        factory.createJobFacetBuilders(context));
  }

  /**
   * Add an {@link ActiveJob} and all of its {@link Stage}s to the maps so we can look them up by id
   * later.
   *
   * @param job
   */
  void registerJob(ActiveJob job) {
    jobMap.put(job.jobId(), job);
    stageMap.put(job.finalStage().id(), job.finalStage());
    job.finalStage().parents().forall(toScalaFn(stage -> stageMap.put(stage.id(), stage)));
  }

  RunEvent buildRun(
      Optional<ParentRunFacet> parentRunFacet,
      RunEventBuilder runEventBuilder,
      JobBuilder jobBuilder,
      SparkListenerStageSubmitted event) {
    Stage stage = stageMap.get(event.stageInfo().stageId());
    RDD<?> rdd = stage.rdd();

    List<Object> nodes = new ArrayList<>();
    nodes.addAll(Arrays.asList(event.stageInfo(), stage));

    nodes.addAll(Rdds.flattenRDDs(rdd));

    return populateRun(parentRunFacet, runEventBuilder, jobBuilder, nodes);
  }

  RunEvent buildRun(
      Optional<ParentRunFacet> parentRunFacet,
      RunEventBuilder runEventBuilder,
      JobBuilder jobBuilder,
      SparkListenerStageCompleted event) {
    Stage stage = stageMap.get(event.stageInfo().stageId());
    RDD<?> rdd = stage.rdd();

    List<Object> nodes = new ArrayList<>();
    nodes.addAll(Arrays.asList(event.stageInfo(), stage));

    nodes.addAll(Rdds.flattenRDDs(rdd));

    return populateRun(parentRunFacet, runEventBuilder, jobBuilder, nodes);
  }

  RunEvent buildRun(
      Optional<ParentRunFacet> parentRunFacet,
      RunEventBuilder runEventBuilder,
      JobBuilder jobBuilder,
      SparkListenerSQLExecutionStart event) {
    runEventBuilder.eventType("START");
    return buildRun(parentRunFacet, runEventBuilder, jobBuilder, event, Optional.empty());
  }

  RunEvent buildRun(
      Optional<ParentRunFacet> parentRunFacet,
      RunEventBuilder runEventBuilder,
      JobBuilder jobBuilder,
      SparkListenerSQLExecutionEnd event) {
    runEventBuilder.eventType("COMPLETE");
    return buildRun(parentRunFacet, runEventBuilder, jobBuilder, event, Optional.empty());
  }

  RunEvent buildRun(
      Optional<ParentRunFacet> parentRunFacet,
      RunEventBuilder runEventBuilder,
      JobBuilder jobBuilder,
      SparkListenerJobStart event) {
    runEventBuilder.eventType("START");
    return buildRun(
        parentRunFacet,
        runEventBuilder,
        jobBuilder,
        event,
        Optional.ofNullable(jobMap.get(event.jobId())));
  }

  RunEvent buildRun(
      Optional<ParentRunFacet> parentRunFacet,
      RunEventBuilder runEventBuilder,
      JobBuilder jobBuilder,
      SparkListenerJobEnd event) {
    runEventBuilder.eventType(event.jobResult() instanceof JobFailed ? "FAIL" : "COMPLETE");
    return buildRun(
        parentRunFacet,
        runEventBuilder,
        jobBuilder,
        event,
        Optional.ofNullable(jobMap.get(event.jobId())));
  }

  private RunEvent buildRun(
      Optional<ParentRunFacet> parentRunFacet,
      RunEventBuilder runEventBuilder,
      JobBuilder jobBuilder,
      Object event,
      Optional<ActiveJob> job) {
    List<Object> nodes = new ArrayList<>();
    nodes.add(event);
    job.ifPresent(
        j -> {
          nodes.add(j);
          nodes.addAll(Rdds.flattenRDDs(j.finalStage().rdd()));
        });

    return populateRun(parentRunFacet, runEventBuilder, jobBuilder, nodes);
  }

  private RunEvent populateRun(
      Optional<ParentRunFacet> parentRunFacet,
      RunEventBuilder runEventBuilder,
      JobBuilder jobBuilder,
      List<Object> nodes) {
    OpenLineage openLineage = openLineageContext.getOpenLineage();

    RunFacetsBuilder runFacetsBuilder = openLineage.newRunFacetsBuilder();
    parentRunFacet.ifPresent(runFacetsBuilder::parent);
    RunFacets runFacets = buildFacets(nodes, runFacetBuilders, runFacetsBuilder.build());
    openLineageContext
        .getQueryExecution()
        .flatMap(qe -> unknownEntryFacetListener.build(qe.optimizedPlan()))
        .ifPresent(facet -> runFacetsBuilder.put("spark_unknown", facet));
    OpenLineage.JobFacets jobFacets =
        buildFacets(nodes, jobFacetBuilders, openLineage.newJobFacets(null, null, null));
    OpenLineage.RunBuilder runBuilder =
        openLineage.newRunBuilder().runId(openLineageContext.getRunUuid()).facets(runFacets);
    List<InputDataset> inputDatasets = buildInputDatasets(nodes);
    List<OutputDataset> outputDatasets = buildOutputDatasets(nodes);

    return runEventBuilder
        .run(runBuilder.build())
        .job(jobBuilder.facets(jobFacets).build())
        .inputs(inputDatasets)
        .outputs(outputDatasets)
        .build();
  }

  private List<OpenLineage.InputDataset> buildInputDatasets(List<Object> nodes) {
    openLineageContext
        .getQueryExecution()
        .ifPresent(
            qe -> {
              if (log.isDebugEnabled()) {
                log.debug("Traversing optimized plan {}", qe.optimizedPlan().toJSON());
                log.debug("Physical plan executed {}", qe.executedPlan().toJSON());
              }
            });
    log.info(
        "Visiting query plan {} with input dataset builders {}",
        openLineageContext.getQueryExecution(),
        inputDatasetQueryPlanVisitors);

    List<OpenLineage.InputDataset> datasets =
        Stream.concat(
                buildDatasets(nodes, inputDatasetBuilders),
                openLineageContext
                    .getQueryExecution()
                    .map(
                        qe ->
                            ScalaConversionUtils.fromSeq(
                                    qe.optimizedPlan()
                                        .collect(
                                            PlanUtils.merge(inputDatasetQueryPlanVisitors)
                                                .orElse(unknownEntryFacetListener)))
                                .stream()
                                .flatMap(List::stream)
                                .map(((Class<InputDataset>) InputDataset.class)::cast))
                    .orElse(Stream.empty()))
            .collect(Collectors.toList());
    OpenLineage openLineage = openLineageContext.getOpenLineage();
    if (!datasets.isEmpty()) {
      Map<String, InputDatasetFacet> inputFacetsMap = new HashMap<>();
      nodes.forEach(
          event -> inputDatasetFacetBuilders.forEach(fn -> fn.accept(event, inputFacetsMap::put)));
      Map<String, DatasetFacets> datasetFacetsMap = new HashMap<>();
      nodes.forEach(
          event -> inputDatasetFacetBuilders.forEach(fn -> fn.accept(event, inputFacetsMap::put)));
      return datasets.stream()
          .map(
              ds ->
                  openLineage
                      .newInputDatasetBuilder()
                      .name(ds.getName())
                      .namespace(ds.getNamespace())
                      .inputFacets(
                          mergeFacets(
                              inputFacetsMap, ds.getInputFacets(), InputDatasetInputFacets.class))
                      .facets(mergeFacets(datasetFacetsMap, ds.getFacets(), DatasetFacets.class))
                      .build())
          .collect(Collectors.toList());
    }
    return datasets;
  }

  private List<OpenLineage.OutputDataset> buildOutputDatasets(List<Object> nodes) {
    log.info(
        "Visiting query plan {} with output dataset builders {}",
        openLineageContext.getQueryExecution(),
        outputDatasetBuilders);
    PartialFunction<LogicalPlan, List<OutputDataset>> visitor =
        PlanUtils.merge(outputDatasetQueryPlanVisitors);
    List<OutputDataset> datasets =
        Stream.concat(
                buildDatasets(nodes, outputDatasetBuilders),
                openLineageContext
                    .getQueryExecution()
                    .map(
                        qe ->
                            visitor.applyOrElse(qe.optimizedPlan(), unknownEntryFacetListener)
                                .stream()
                                .map(OutputDataset.class::cast))
                    .orElse(Stream.empty()))
            .collect(Collectors.toList());

    OpenLineage openLineage = openLineageContext.getOpenLineage();

    if (!datasets.isEmpty()) {
      Map<String, OutputDatasetFacet> outputFacetsMap = new HashMap<>();
      nodes.forEach(
          event ->
              outputDatasetFacetBuilders.forEach(fn -> fn.accept(event, outputFacetsMap::put)));
      Map<String, DatasetFacet> datasetFacetsMap = new HashMap<>();
      nodes.forEach(
          event -> datasetFacetBuilders.forEach(fn -> fn.accept(event, datasetFacetsMap::put)));
      return datasets.stream()
          .map(
              ds ->
                  openLineage
                      .newOutputDatasetBuilder()
                      .name(ds.getName())
                      .namespace(ds.getNamespace())
                      .outputFacets(
                          mergeFacets(
                              outputFacetsMap,
                              ds.getOutputFacets(),
                              OutputDatasetOutputFacets.class))
                      .facets(mergeFacets(datasetFacetsMap, ds.getFacets(), DatasetFacets.class))
                      .build())
          .collect(Collectors.toList());
    }
    return datasets;
  }

  private <T> Stream<T> buildDatasets(
      List<Object> nodes, List<PartialFunction<Object, List<T>>> builders) {
    PartialFunction<Object, List<T>> fn = PlanUtils.merge(builders);
    return nodes.stream()
        .flatMap(
            event -> {
              if (fn.isDefinedAt(event)) {
                return fn.apply(event).stream();
              } else {
                return Stream.empty();
              }
            });
  }

  /**
   * Attach facets to a facet container, such as an {@link
   * io.openlineage.client.OpenLineage.InputDatasetInputFacets} or an {@link
   * io.openlineage.client.OpenLineage.OutputDatasetOutputFacets}. Facets returned by a {@link
   * CustomFacetBuilder} may be attached to a field in the container, such as {@link
   * io.openlineage.client.OpenLineage.InputDatasetInputFacets#dataQualityMetrics} or may be
   * attached as a key/value pair in the {@link
   * io.openlineage.client.OpenLineage.InputDatasetInputFacets#additionalProperties} map. The
   * serialized JSON does not distinguish between these, but the java class does. The Java class
   * also has some fields, such as the {@link
   * io.openlineage.client.OpenLineage.InputDatasetInputFacets#producer} URI, which need to be
   * included in the serialized JSON.
   *
   * <p>This method will collect the custom facets generated by the builders into a Map, then merge
   * the properties into the facet container, using the {@link ObjectMapper#updateValue(Object,
   * Object)} method.
   *
   * @param events
   * @param builders
   * @param facetsContainer
   * @param <T>
   * @param <F>
   * @return
   */
  private <T, F> F buildFacets(
      List<Object> events, List<CustomFacetBuilder<?, ? extends T>> builders, F facetsContainer) {
    Map<String, T> facetsMap = new HashMap<>();
    events.forEach(event -> builders.forEach(fn -> fn.accept(event, facetsMap::put)));
    try {
      return objectMapper.updateValue(facetsContainer, facetsMap);
    } catch (JsonMappingException e) {
      throw new RuntimeException(e);
    }
  }

  private <T, F> T mergeFacets(Map<String, F> facetsMap, T facetsContainer, Class<T> klass) {
    if (facetsContainer == null) {
      return objectMapper.convertValue(facetsMap, klass);
    }
    try {
      return objectMapper.updateValue(facetsContainer, facetsMap);
    } catch (JsonMappingException e) {
      throw new RuntimeException(e);
    }
  }
}
