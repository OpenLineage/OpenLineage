package io.openlineage.spark.agent.lifecycle;

import static io.openlineage.spark.agent.util.ScalaConversionUtils.toScalaFn;

import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.DatasetFacet;
import io.openlineage.client.OpenLineage.ParentRunFacet;
import io.openlineage.client.OpenLineage.RunEventBuilder;
import io.openlineage.spark.agent.lifecycle.Rdds;
import io.openlineage.spark.agent.util.PlanUtils;
import io.openlineage.spark.api.CustomFacetBuilder;
import io.openlineage.spark.api.OpenLineageContext;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.AllArgsConstructor;
import lombok.NonNull;
import org.apache.spark.rdd.RDD;
import org.apache.spark.scheduler.ActiveJob;
import org.apache.spark.scheduler.JobFailed;
import org.apache.spark.scheduler.SparkListenerJobEnd;
import org.apache.spark.scheduler.SparkListenerJobStart;
import org.apache.spark.scheduler.SparkListenerStageCompleted;
import org.apache.spark.scheduler.Stage;
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
 * <p>Any {@link io.openlineage.client.OpenLineage.RunFacet}s and {@link
 * io.openlineage.client.OpenLineage.JobFacet}s returned by the {@link CustomFacetBuilder}s are
 * appended to the {@link io.openlineage.client.OpenLineage.Run} and {@link
 * io.openlineage.client.OpenLineage.Job}, respectively.
 *
 * <p><b>If</b> any {@link io.openlineage.client.OpenLineage.InputDatasetBuilder}s or {@link
 * io.openlineage.client.OpenLineage.OutputDatasetBuilder}s are returned from the partial functions,
 * the {@link #inputDatasetBuilders} or {@link #outputDatasetBuilders} will be invoked using the
 * same input arguments in order to construct any {@link
 * io.openlineage.client.OpenLineage.InputDatasetFacet}s or {@link
 * io.openlineage.client.OpenLineage.OutputDatasetFacet}s to the returned dataset. {@link
 * io.openlineage.client.OpenLineage.InputDatasetFacet}s and {@link
 * io.openlineage.client.OpenLineage.OutputDatasetFacet}s will be attached to <i>any</i> {@link
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
@AllArgsConstructor
class OpenLineageRunEventBuilder {

  @NonNull private final OpenLineageContext openLineageContext;

  @NonNull
  private final List<PartialFunction<Object, List<OpenLineage.InputDatasetBuilder>>>
      inputDatasetBuilders;

  @NonNull
  private final List<PartialFunction<Object, List<OpenLineage.OutputDatasetBuilder>>>
      outputDatasetBuilders;

  @NonNull private final List<CustomFacetBuilder<Object, DatasetFacet>> datasetFacetBuilders;

  @NonNull
  private final List<CustomFacetBuilder<Object, OpenLineage.InputDatasetFacet>>
      inputDatasetFacetBuilders;

  @NonNull
  private final List<CustomFacetBuilder<Object, OpenLineage.OutputDatasetFacet>>
      outputDatasetFacetBuilders;

  @NonNull private final List<CustomFacetBuilder<Object, OpenLineage.RunFacet>> runFacetBuilders;
  @NonNull private final List<CustomFacetBuilder<Object, OpenLineage.JobFacet>> jobFacetBuilders;

  private final Map<Integer, ActiveJob> jobMap = new HashMap<>();
  private final Map<Integer, Stage> stageMap = new HashMap<>();

  private final ObjectMapper objectMapper = new ObjectMapper();

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

  void buildRun(
      OpenLineage.ParentRunFacet parentRunFacet,
      OpenLineage.RunEventBuilder runEventBuilder,
      SparkListenerStageCompleted event) {
    Stage stage = stageMap.get(event.stageInfo().stageId());
    RDD<?> rdd = stage.rdd();

    List<Object> nodes = new ArrayList<>();
    nodes.addAll(Arrays.asList(event.stageInfo(), stage));

    nodes.addAll(Rdds.flattenRDDs(rdd));

    populateRun(parentRunFacet, runEventBuilder, nodes);
  }

  void buildRun(
      OpenLineage.ParentRunFacet parentRunFacet,
      OpenLineage.RunEventBuilder runEventBuilder,
      SparkListenerJobStart event) {
    runEventBuilder.eventType("START");
    buildRunFromJob(parentRunFacet, runEventBuilder, event, jobMap.get(event.jobId()));
  }

  void buildRun(
      OpenLineage.ParentRunFacet parentRunFacet,
      OpenLineage.RunEventBuilder runEventBuilder,
      SparkListenerJobEnd event) {
    runEventBuilder.eventType(event.jobResult() instanceof JobFailed ? "FAIL" : "COMPLETE");
    buildRunFromJob(parentRunFacet, runEventBuilder, event, jobMap.get(event.jobId()));
  }

  private void buildRunFromJob(
      ParentRunFacet parentRunFacet, RunEventBuilder runEventBuilder, Object event, ActiveJob job) {
    RDD<?> rdd = job.finalStage().rdd();

    List<Object> nodes = new ArrayList<>();
    nodes.addAll(Arrays.asList(event, job));

    nodes.addAll(Rdds.flattenRDDs(rdd));

    populateRun(parentRunFacet, runEventBuilder, nodes);
  }

  private void populateRun(
      OpenLineage.ParentRunFacet parentRunFacet,
      OpenLineage.RunEventBuilder runEventBuilder,
      List<Object> nodes) {
    OpenLineage openLineage = openLineageContext.getOpenLineage();
    OpenLineage.RunFacets runFacets =
        buildFacets(nodes, runFacetBuilders, openLineage.newRunFacetsBuilder())
            .parent(parentRunFacet)
            .build();
    OpenLineage.JobFacets jobFacets =
        buildFacets(nodes, jobFacetBuilders, openLineage.newJobFacetsBuilder()).build();
    OpenLineage.RunBuilder runBuilder = openLineage.newRunBuilder().facets(runFacets);
    OpenLineage.JobBuilder jobBuilder = openLineage.newJobBuilder().facets(jobFacets);

    runEventBuilder
        .run(runBuilder.build())
        .job(jobBuilder.build())
        .inputs(buildInputDatasets(nodes))
        .outputs(buildOutputDatasets(nodes));
  }

  private List<OpenLineage.InputDataset> buildInputDatasets(List<Object> nodes) {
    List<OpenLineage.InputDatasetBuilder> datasets = buildDatasets(nodes, inputDatasetBuilders);
    OpenLineage openLineage = openLineageContext.getOpenLineage();
    if (!datasets.isEmpty()) {
      OpenLineage.InputDatasetInputFacets inputFacets =
          buildFacets(
                  nodes, inputDatasetFacetBuilders, openLineage.newInputDatasetInputFacetsBuilder())
              .build();
      OpenLineage.DatasetFacets datasetFacets =
          buildFacets(nodes, datasetFacetBuilders, openLineage.newDatasetFacetsBuilder()).build();
      datasets.forEach(ds -> ds.inputFacets(inputFacets).facets(datasetFacets));
    }
    return datasets.stream()
        .map(OpenLineage.InputDatasetBuilder::build)
        .collect(Collectors.toList());
  }

  private List<OpenLineage.OutputDataset> buildOutputDatasets(List<Object> nodes) {
    List<OpenLineage.OutputDatasetBuilder> datasets = buildDatasets(nodes, outputDatasetBuilders);
    OpenLineage openLineage = openLineageContext.getOpenLineage();
    if (!datasets.isEmpty()) {
      OpenLineage.OutputDatasetOutputFacets outputFacets =
          buildFacets(
                  nodes,
                  outputDatasetFacetBuilders,
                  openLineage.newOutputDatasetOutputFacetsBuilder())
              .build();
      OpenLineage.DatasetFacets datasetFacets =
          buildFacets(nodes, datasetFacetBuilders, openLineage.newDatasetFacetsBuilder()).build();
      datasets.forEach(ds -> ds.outputFacets(outputFacets).facets(datasetFacets));
    }
    return datasets.stream()
        .map(OpenLineage.OutputDatasetBuilder::build)
        .collect(Collectors.toList());
  }

  private <T> List<T> buildDatasets(
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
            })
        .collect(Collectors.toList());
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
      List<Object> events, List<CustomFacetBuilder<Object, T>> builders, F facetsContainer) {
    Map<String, T> facetsMap = new HashMap<>();
    events.forEach(event -> builders.forEach(fn -> fn.accept(event, facetsMap::put)));
    try {
      return objectMapper.updateValue(facetsContainer, facetsMap);
    } catch (JsonMappingException e) {
      throw new RuntimeException(e);
    }
  }
}
