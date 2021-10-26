package io.openlineage.spark.agent.lifecycle;

import static io.openlineage.spark.agent.util.PlanUtils.merge;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.InputDataset;
import io.openlineage.spark.agent.JobMetricsHolder;
import io.openlineage.spark.agent.OpenLineageContext;
import io.openlineage.spark.agent.client.OpenLineageClient;
import io.openlineage.spark.agent.facets.ErrorFacet;
import io.openlineage.spark.agent.facets.LogicalPlanFacet;
import io.openlineage.spark.agent.facets.SparkVersionFacet;
import io.openlineage.spark.agent.facets.UnknownEntryFacet;
import io.openlineage.spark.agent.lifecycle.plan.QueryPlanVisitor;
import io.openlineage.spark.agent.util.PlanUtils;
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
import org.apache.spark.sql.execution.SQLExecution;
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

  private OpenLineageContext sparkContext;
  private final List<QueryPlanVisitor<LogicalPlan, OpenLineage.OutputDataset>>
      outputDatasetSupplier;
  private final List<QueryPlanVisitor<LogicalPlan, OpenLineage.InputDataset>> inputDatasetSupplier;
  private JobMetricsHolder jobMetrics;

  public SparkSQLExecutionContext(
      long executionId,
      OpenLineageContext sparkContext,
      List<QueryPlanVisitor<LogicalPlan, OpenLineage.OutputDataset>> outputDatasetSupplier,
      List<QueryPlanVisitor<LogicalPlan, OpenLineage.InputDataset>> inputDatasetSupplier,
      JobMetricsHolder jobMetrics) {
    this.executionId = executionId;
    this.sparkContext = sparkContext;
    this.queryExecution = SQLExecution.getQueryExecution(executionId);
    this.outputDatasetSupplier = outputDatasetSupplier;
    this.inputDatasetSupplier = inputDatasetSupplier;
    this.jobMetrics = jobMetrics;
  }

  public void start(SparkListenerSQLExecutionStart startEvent) {}

  public void end(SparkListenerSQLExecutionEnd endEvent) {}

  @Override
  public void setActiveJob(ActiveJob activeJob) {}

  @Override
  public void start(SparkListenerJobStart jobStart) {
    log.info("Starting job as part of spark-sql:" + jobStart.jobId());
    if (queryExecution == null) {
      log.info("No execution info {}", queryExecution);
      return;
    }

    PartialFunction<LogicalPlan, List<OpenLineage.OutputDataset>> outputVisitor =
        merge(outputDatasetSupplier);

    PartialFunction<LogicalPlan, List<OpenLineage.OutputDataset>> planTraversal =
        getPlanTraversal(outputVisitor);
    List<OpenLineage.OutputDataset> outputDatasets =
        planTraversal.isDefinedAt(queryExecution.optimizedPlan())
            ? planTraversal.apply(queryExecution.optimizedPlan())
            : Collections.emptyList();

    List<InputDataset> inputDatasets = getInputDatasets();
    UnknownEntryFacet unknownFacet =
        unknownEntryFacetListener.build(queryExecution.optimizedPlan()).orElse(null);

    OpenLineage ol = new OpenLineage(OpenLineageClient.OPEN_LINEAGE_CLIENT_URI);
    OpenLineage.RunEvent event =
        ol.newRunEventBuilder()
            .eventTime(toZonedTime(jobStart.time()))
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

    log.debug("Posting event for start {}: {}", jobStart, event);
    sparkContext.emit(event);
  }

  private List<InputDataset> getInputDatasets() {
    PartialFunction<LogicalPlan, List<InputDataset>> inputFunc =
        PlanUtils.merge(inputDatasetSupplier);
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
    return sparkContext
        .getParentRunId()
        .map(
            runId ->
                PlanUtils.parentRunFacet(
                    runId, sparkContext.getParentJobName(), sparkContext.getJobNamespace()));
  }

  @Override
  public void end(SparkListenerJobEnd jobEnd) {
    log.info("Ending job as part of spark-sql:" + jobEnd.jobId());
    if (queryExecution == null) {
      log.info("No execution info {}", queryExecution);
      return;
    }
    if (log.isDebugEnabled()) {
      log.debug("Traversing optimized plan {}", queryExecution.optimizedPlan().toJSON());
      log.debug("Physical plan executed {}", queryExecution.executedPlan().toJSON());
    }
    PartialFunction<LogicalPlan, List<OpenLineage.OutputDataset>> outputVisitor =
        merge(outputDatasetSupplier);
    PartialFunction<LogicalPlan, List<OpenLineage.OutputDataset>> planTraversal =
        getPlanTraversal(outputVisitor);
    List<OpenLineage.OutputDataset> outputDatasets =
        planTraversal.isDefinedAt(queryExecution.optimizedPlan())
            ? planTraversal.apply(queryExecution.optimizedPlan())
            : Collections.emptyList();
    outputDatasets = populateOutputMetrics(jobEnd.jobId(), outputDatasets);

    List<InputDataset> inputDatasets = getInputDatasets();
    UnknownEntryFacet unknownFacet =
        unknownEntryFacetListener.build(queryExecution.optimizedPlan()).orElse(null);

    OpenLineage ol = new OpenLineage(OpenLineageClient.OPEN_LINEAGE_CLIENT_URI);
    OpenLineage.RunEvent event =
        ol.newRunEventBuilder()
            .eventTime(toZonedTime(jobEnd.time()))
            .eventType(getEventType(jobEnd.jobResult()))
            .inputs(inputDatasets)
            .outputs(outputDatasets)
            .run(
                buildRun(
                    buildRunFacets(
                        buildParentFacet(),
                        new SimpleImmutableEntry(
                            "spark.logicalPlan", buildLogicalPlanFacet(queryExecution.logical())),
                        new SimpleImmutableEntry(
                            "spark.exception", buildJobErrorFacet(jobEnd.jobResult())),
                        new SimpleImmutableEntry("spark_unknown", unknownFacet),
                        new SimpleImmutableEntry(
                            "spark_version", new SparkVersionFacet(SparkSession.active())))))
            .job(buildJob(queryExecution))
            .build();

    log.debug("Posting event for start {}: {}", jobEnd, event);
    sparkContext.emit(event);
  }

  private List<OpenLineage.OutputDataset> populateOutputMetrics(
      int jobId, List<OpenLineage.OutputDataset> outputDatasets) {
    if (outputDatasets.isEmpty()) return outputDatasets;
    OpenLineage ol = new OpenLineage(OpenLineageClient.OPEN_LINEAGE_CLIENT_URI);

    OpenLineage.OutputDataset outputDataset = outputDatasets.get(0);

    Map<JobMetricsHolder.Metric, Number> metrics = jobMetrics.pollMetrics(jobId);

    OpenLineage.OutputDatasetOutputFacetsBuilder statisticsOutputFacets =
        ol.newOutputDatasetOutputFacetsBuilder()
            .outputStatistics(
                ol.newOutputStatisticsOutputDatasetFacet(
                    metrics.getOrDefault(JobMetricsHolder.Metric.WRITE_RECORDS, 0L).longValue(),
                    metrics.getOrDefault(JobMetricsHolder.Metric.WRITE_BYTES, 0L).longValue()));
    OpenLineage.OutputDatasetOutputFacets outputFacets = outputDataset.getOutputFacets();
    if (outputFacets != null) {
      for (Map.Entry<String, OpenLineage.OutputDatasetFacet> entry :
          outputDataset.getOutputFacets().getAdditionalProperties().entrySet()) {
        statisticsOutputFacets.put(entry.getKey(), entry.getValue());
      }
    }
    return Collections.singletonList(
        ol.newOutputDataset(
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
    return new OpenLineage.RunBuilder().runId(runUuid).facets(facets).build();
  }

  protected OpenLineage.RunFacets buildRunFacets(
      Optional<OpenLineage.ParentRunFacet> parentRunFacet,
      Map.Entry<String, OpenLineage.DefaultRunFacet>... customFacets) {
    OpenLineage.RunFacetsBuilder builder = new OpenLineage.RunFacetsBuilder();
    parentRunFacet.ifPresent(builder::parent);
    Arrays.stream(customFacets)
        .filter(e -> Objects.nonNull(e.getValue()))
        .forEach(e -> builder.put(e.getKey(), e.getValue()));
    return builder.build();
  }

  protected LogicalPlanFacet buildLogicalPlanFacet(LogicalPlan plan) {
    return LogicalPlanFacet.builder().plan(plan).build();
  }

  protected ErrorFacet buildJobErrorFacet(JobResult jobResult) {
    if (jobResult instanceof JobFailed && ((JobFailed) jobResult).exception() != null) {
      return ErrorFacet.builder().exception(((JobFailed) jobResult).exception()).build();
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
    return new OpenLineage.JobBuilder()
        .namespace(this.sparkContext.getJobNamespace())
        .name(
            sparkContext.appName().replaceAll(CAMEL_TO_SNAKE_CASE, "_$1").toLowerCase(Locale.ROOT)
                + "."
                + node.nodeName().replaceAll(CAMEL_TO_SNAKE_CASE, "_$1").toLowerCase(Locale.ROOT))
        .build();
  }
}
