package io.openlineage.spark.agent.lifecycle;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.InputDataset;
import io.openlineage.client.OpenLineage.OutputDataset;
import io.openlineage.spark.agent.EventEmitter;
import io.openlineage.spark.agent.JobMetricsHolder;
import io.openlineage.spark.agent.client.OpenLineageClient;
import io.openlineage.spark.agent.facets.ErrorFacet;
import io.openlineage.spark.agent.facets.LogicalPlanFacet;
import io.openlineage.spark.agent.facets.SparkVersionFacet;
import io.openlineage.spark.agent.facets.UnknownEntryFacet;
import io.openlineage.spark.agent.util.PlanUtils;
import io.openlineage.spark.api.OpenLineageContext;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkContext;
import org.apache.spark.scheduler.ActiveJob;
import org.apache.spark.scheduler.JobFailed;
import org.apache.spark.scheduler.JobResult;
import org.apache.spark.scheduler.SparkListenerJobEnd;
import org.apache.spark.scheduler.SparkListenerJobStart;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.QueryExecution;
import org.apache.spark.sql.execution.SparkPlan;
import org.apache.spark.sql.execution.WholeStageCodegenExec;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart;
import scala.PartialFunction;
import scala.collection.JavaConversions;

@Slf4j
public class SparkSQLExecutionContext implements ExecutionContext {

  private final long executionId;
  private final QueryExecution queryExecution;
  private final UUID runUuid = UUID.randomUUID();
  private final UnknownEntryFacetListener unknownEntryFacetListener =
      new UnknownEntryFacetListener();

  private EventEmitter eventEmitter;
  private final OpenLineageContext context;
  private final JobMetricsHolder jobMetrics;
  private final OpenLineage openLineage =
      new OpenLineage(OpenLineageClient.OPEN_LINEAGE_CLIENT_URI);

  private AtomicBoolean started = new AtomicBoolean(false);
  private AtomicBoolean finished = new AtomicBoolean(false);
  private Optional<Integer> jobId = Optional.empty();

  public SparkSQLExecutionContext(
      long executionId,
      EventEmitter eventEmitter,
      QueryExecution queryExecution,
      OpenLineageContext context) {
    this.executionId = executionId;
    this.eventEmitter = eventEmitter;
    this.queryExecution = queryExecution;
    this.context = context;
    this.jobMetrics = JobMetricsHolder.getInstance();
  }

  public void start(SparkListenerSQLExecutionStart startEvent) {
    log.debug("SparkListenerSQLExecutionStart - executionId: {}", startEvent.executionId());
    startEvent(startEvent.time());
  }

  public void end(SparkListenerSQLExecutionEnd endEvent) {
    log.debug("SparkListenerSQLExecutionEnd - executionId: {}", endEvent.executionId());
    // TODO: can we get failed event here?
    // If not, then we probably need to use this only for LogicalPlans that emit no Job events.
    // Maybe use QueryExecutionListener?
    endEvent(endEvent.time(), "COMPLETE", null);
  }

  @Override
  public void setActiveJob(ActiveJob activeJob) {}

  @Override
  public void start(SparkListenerJobStart jobStart) {
    log.debug("SparkListenerJobStart - executionId: " + executionId);
    jobId = Optional.of(jobStart.jobId());
    startEvent(jobStart.time());
  }

  @Override
  public void end(SparkListenerJobEnd jobEnd) {
    log.debug("SparkListenerJobEnd - executionId: " + executionId);
    Exception exception = null;
    if (jobEnd.jobResult() instanceof JobFailed) {
      exception = ((JobFailed) jobEnd.jobResult()).exception();
    }
    jobId = Optional.of(jobEnd.jobId());
    endEvent(jobEnd.time(), getEventType(jobEnd.jobResult()), exception);
  }

  void startEvent(Long time) {
    if (!started.compareAndSet(false, true)) {
      log.debug("Start event already emitted: returning");
      return;
    }

    if (queryExecution == null) {
      log.info("No execution info {}", queryExecution);
      return;
    }

    PartialFunction<LogicalPlan, List<OutputDataset>> outputVisitor =
        PlanUtils.merge(context.getOutputDatasetQueryPlanVisitors());

    PartialFunction<LogicalPlan, List<OutputDataset>> planTraversal =
        getPlanTraversal(outputVisitor);
    List<OutputDataset> outputDatasets =
        planTraversal.isDefinedAt(queryExecution.optimizedPlan())
            ? planTraversal.apply(queryExecution.optimizedPlan())
            : Collections.emptyList();

    List<InputDataset> inputDatasets = getInputDatasets();
    UnknownEntryFacet unknownFacet =
        unknownEntryFacetListener.build(queryExecution.optimizedPlan()).orElse(null);

    OpenLineage.RunEvent event =
        openLineage
            .newRunEventBuilder()
            .eventTime(toZonedTime(time))
            .eventType("START")
            .inputs(inputDatasets)
            .outputs(outputDatasets)
            .run(
                buildRun(
                    buildRunFacets(
                        buildParentFacet(),
                        new SimpleImmutableEntry(
                            "spark.logicalPlan",
                            buildLogicalPlanFacet(queryExecution.optimizedPlan())),
                        new SimpleImmutableEntry("spark_unknown", unknownFacet),
                        new SimpleImmutableEntry(
                            "spark_version", new SparkVersionFacet(SparkSession.active())))))
            .job(buildJob(queryExecution))
            .build();

    log.debug("Posting event for start {}: {}", executionId, event);
    eventEmitter.emit(event);
  }

  private List<InputDataset> getInputDatasets() {
    PartialFunction<LogicalPlan, List<InputDataset>> inputFunc =
        PlanUtils.merge(context.getInputDatasetQueryPlanVisitors());
    return JavaConversions.seqAsJavaList(
            queryExecution.optimizedPlan().collect(getPlanTraversal(inputFunc)))
        .stream()
        .filter(Objects::nonNull)
        .flatMap(List::stream)
        .collect(Collectors.toList());
  }

  private <T> PlanTraversal<LogicalPlan, List<T>> getPlanTraversal(
      PartialFunction<LogicalPlan, List<T>> processor) {
    return PlanTraversal.<LogicalPlan, List<T>>builder()
        .processor(processor)
        .visitedNodeListener(unknownEntryFacetListener)
        .build();
  }

  private Optional<OpenLineage.ParentRunFacet> buildParentFacet() {
    return eventEmitter
        .getParentRunId()
        .map(
            runId ->
                PlanUtils.parentRunFacet(
                    runId, eventEmitter.getParentJobName(), eventEmitter.getJobNamespace()));
  }

  void endEvent(Long time, String eventType, Exception exception) {
    if (!finished.compareAndSet(false, true)) {
      log.debug("Event already finished, returning");
      return;
    }

    if (queryExecution == null) {
      log.info("No execution info {}", queryExecution);
      return;
    }
    LogicalPlan optimizedPlan = queryExecution.optimizedPlan();
    if (log.isDebugEnabled()) {
      log.debug("Traversing optimized plan {}", optimizedPlan.toJSON());
      log.debug("Physical plan executed {}", queryExecution.executedPlan().toJSON());
    }

    PartialFunction<LogicalPlan, List<OutputDataset>> outputVisitor =
        PlanUtils.merge(context.getOutputDatasetQueryPlanVisitors());
    PartialFunction<LogicalPlan, List<OutputDataset>> planTraversal =
        getPlanTraversal(outputVisitor);
    List<OutputDataset> outputDatasets =
        planTraversal.isDefinedAt(optimizedPlan)
            ? planTraversal.apply(optimizedPlan)
            : Collections.emptyList();
    if (jobId.isPresent()) {
      outputDatasets = populateOutputMetrics(jobId.get(), outputDatasets);
    }

    List<InputDataset> inputDatasets = getInputDatasets();
    UnknownEntryFacet unknownFacet = unknownEntryFacetListener.build(optimizedPlan).orElse(null);

    OpenLineage.RunEvent event =
        openLineage
            .newRunEventBuilder()
            .eventTime(toZonedTime(time))
            .eventType(eventType)
            .inputs(inputDatasets)
            .outputs(outputDatasets)
            .run(
                buildRun(
                    buildRunFacets(
                        buildParentFacet(),
                        new SimpleImmutableEntry(
                            "spark.logicalPlan", buildLogicalPlanFacet(queryExecution.logical())),
                        new SimpleImmutableEntry(
                            "spark.exception", buildJobErrorFacet(eventType, exception)),
                        new SimpleImmutableEntry("spark_unknown", unknownFacet),
                        new SimpleImmutableEntry(
                            "spark_version", new SparkVersionFacet(SparkSession.active())))))
            .job(buildJob(queryExecution))
            .build();

    log.debug("Posting event for end {}: {}", executionId, event);
    eventEmitter.emit(event);
  }

  private List<OutputDataset> populateOutputMetrics(int jobId, List<OutputDataset> outputDatasets) {
    if (outputDatasets.isEmpty()) return outputDatasets;

    OutputDataset outputDataset = outputDatasets.get(0);

    Map<JobMetricsHolder.Metric, Number> metrics = jobMetrics.pollMetrics(jobId);

    Optional<OpenLineage.OutputStatisticsOutputDatasetFacet> outputStats = Optional.empty();
    if (metrics.containsKey(JobMetricsHolder.Metric.WRITE_BYTES)
        || metrics.containsKey(JobMetricsHolder.Metric.WRITE_RECORDS)) {
      outputStats =
          Optional.of(
              openLineage.newOutputStatisticsOutputDatasetFacet(
                  Optional.of(metrics.get(JobMetricsHolder.Metric.WRITE_RECORDS))
                      .map(Number::longValue)
                      .orElse(null),
                  Optional.of(metrics.get(JobMetricsHolder.Metric.WRITE_BYTES))
                      .map(Number::longValue)
                      .orElse(null)));
    }
    OpenLineage.OutputDatasetOutputFacetsBuilder statisticsOutputFacets =
        openLineage
            .newOutputDatasetOutputFacetsBuilder()
            .outputStatistics(outputStats.orElse(null));
    OpenLineage.OutputDatasetOutputFacets outputFacets = outputDataset.getOutputFacets();
    if (outputFacets != null) {
      for (Map.Entry<String, OpenLineage.OutputDatasetFacet> entry :
          outputDataset.getOutputFacets().getAdditionalProperties().entrySet()) {
        statisticsOutputFacets.put(entry.getKey(), entry.getValue());
      }
    }
    return Collections.singletonList(
        openLineage.newOutputDataset(
            outputDataset.getNamespace(),
            outputDataset.getName(),
            outputDataset.getFacets(),
            statisticsOutputFacets.build()));
  }

  protected ZonedDateTime toZonedTime(long time) {
    Instant i = Instant.ofEpochMilli(time);
    return ZonedDateTime.ofInstant(i, ZoneOffset.UTC);
  }

  protected String getEventType(JobResult jobResult) {
    if (jobResult.getClass().getSimpleName().startsWith("JobSucceeded")) {
      return "COMPLETE";
    }
    return "FAIL";
  }

  protected OpenLineage.Run buildRun(OpenLineage.RunFacets facets) {
    return openLineage.newRun(runUuid, facets);
  }

  protected OpenLineage.RunFacets buildRunFacets(
      Optional<OpenLineage.ParentRunFacet> parentRunFacet,
      Map.Entry<String, OpenLineage.DefaultRunFacet>... customFacets) {
    OpenLineage.RunFacetsBuilder builder = openLineage.newRunFacetsBuilder();
    parentRunFacet.ifPresent(builder::parent);
    Arrays.stream(customFacets)
        .filter(e -> Objects.nonNull(e.getValue()))
        .forEach(e -> builder.put(e.getKey(), e.getValue()));
    return builder.build();
  }

  protected LogicalPlanFacet buildLogicalPlanFacet(LogicalPlan plan) {
    return LogicalPlanFacet.builder().plan(plan).build();
  }

  protected ErrorFacet buildJobErrorFacet(String jobResult, Exception exception) {
    if (jobResult.equals("FAIL") && exception != null) {
      return ErrorFacet.builder().exception(exception).build();
    }
    return null;
  }

  protected OpenLineage.Job buildJob(QueryExecution queryExecution) {
    SparkContext sparkContext = queryExecution.executedPlan().sparkContext();
    SparkPlan node = queryExecution.executedPlan();

    // Unwrap SparkPlan from WholeStageCodegen, as that's not a descriptive or helpful job name
    if (node instanceof WholeStageCodegenExec) {
      node = ((WholeStageCodegenExec) node).child();
    }
    return openLineage
        .newJobBuilder()
        .namespace(this.eventEmitter.getJobNamespace())
        .name(
            sparkContext.appName().replaceAll(CAMEL_TO_SNAKE_CASE, "_$1").toLowerCase(Locale.ROOT)
                + "."
                + node.nodeName().replaceAll(CAMEL_TO_SNAKE_CASE, "_$1").toLowerCase(Locale.ROOT))
        .build();
  }
}
