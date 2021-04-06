package marquez.spark.agent.lifecycle;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import marquez.spark.agent.MarquezContext;
import marquez.spark.agent.client.LineageEvent;
import marquez.spark.agent.client.LineageEvent.Dataset;
import marquez.spark.agent.client.LineageEvent.Job;
import marquez.spark.agent.client.LineageEvent.ParentRunFacet;
import marquez.spark.agent.client.LineageEvent.RunFacet;
import marquez.spark.agent.client.OpenLineageClient;
import marquez.spark.agent.facets.ErrorFacet;
import marquez.spark.agent.facets.LogicalPlanFacet;
import marquez.spark.agent.lifecycle.plan.PlanUtils;
import org.apache.spark.SparkContext;
import org.apache.spark.scheduler.ActiveJob;
import org.apache.spark.scheduler.JobFailed;
import org.apache.spark.scheduler.JobResult;
import org.apache.spark.scheduler.SparkListenerJobEnd;
import org.apache.spark.scheduler.SparkListenerJobStart;
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

  private MarquezContext marquezContext;
  private final List<PartialFunction<LogicalPlan, List<Dataset>>> outputDatasetSupplier;
  private final List<PartialFunction<LogicalPlan, List<Dataset>>> inputDatasetSupplier;

  public SparkSQLExecutionContext(
      long executionId,
      MarquezContext marquezContext,
      List<PartialFunction<LogicalPlan, List<Dataset>>> outputDatasetSupplier,
      List<PartialFunction<LogicalPlan, List<Dataset>>> inputDatasetSupplier) {
    this.executionId = executionId;
    this.marquezContext = marquezContext;
    this.queryExecution = SQLExecution.getQueryExecution(executionId);
    this.outputDatasetSupplier = outputDatasetSupplier;
    this.inputDatasetSupplier = inputDatasetSupplier;
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
    List<Dataset> outputDatasets =
        PlanUtils.applyFirst(outputDatasetSupplier, queryExecution.logical());
    List<Dataset> inputDatasets =
        JavaConversions.seqAsJavaList(
                queryExecution.logical().collect(PlanUtils.merge(inputDatasetSupplier)))
            .stream()
            .flatMap(List::stream)
            .collect(Collectors.toList());
    LineageEvent event =
        LineageEvent.builder()
            .inputs(inputDatasets)
            .outputs(outputDatasets)
            .run(
                buildRun(
                    buildRunFacets(
                        buildLogicalPlanFacet(queryExecution.logical()), null, buildParentFacet())))
            .job(buildJob(queryExecution))
            .eventTime(toZonedTime(jobStart.time()))
            .eventType("START")
            .producer(OpenLineageClient.OPEN_LINEAGE_CLIENT_URI)
            .build();

    marquezContext.emit(event);
  }

  private ParentRunFacet buildParentFacet() {
    return PlanUtils.parentRunFacet(
        marquezContext.getParentRunId(),
        marquezContext.getJobName(),
        marquezContext.getJobNamespace());
  }

  @Override
  public void end(SparkListenerJobEnd jobEnd) {
    log.info("Ending job as part of spark-sql:" + jobEnd.jobId());
    if (queryExecution == null) {
      log.info("No execution info {}", queryExecution);
      return;
    }
    log.debug("Traversing logical plan {}", queryExecution.logical().toJSON());
    log.debug("Physical plan executed {}", queryExecution.executedPlan().toJSON());
    List<Dataset> outputDatasets =
        PlanUtils.applyFirst(outputDatasetSupplier, queryExecution.logical());
    List<Dataset> inputDatasets =
        JavaConversions.seqAsJavaList(
                queryExecution.logical().collect(PlanUtils.merge(inputDatasetSupplier)))
            .stream()
            .flatMap(List::stream)
            .collect(Collectors.toList());
    LineageEvent event =
        LineageEvent.builder()
            .inputs(inputDatasets)
            .outputs(outputDatasets)
            .run(
                buildRun(
                    buildRunFacets(
                        buildLogicalPlanFacet(queryExecution.logical()),
                        buildJobErrorFacet(jobEnd.jobResult()),
                        buildParentFacet())))
            .job(buildJob(queryExecution))
            .eventTime(toZonedTime(jobEnd.time()))
            .eventType(getEventType(jobEnd.jobResult()))
            .producer(OpenLineageClient.OPEN_LINEAGE_CLIENT_URI)
            .build();

    marquezContext.emit(event);
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

  protected LineageEvent.Run buildRun(RunFacet facets) {
    return LineageEvent.Run.builder().runId(runUuid.toString()).facets(facets).build();
  }

  protected RunFacet buildRunFacets(
      LogicalPlanFacet logicalPlanFacet, ErrorFacet jobError, ParentRunFacet parentRunFacet) {
    Map<String, Object> additionalFacets = new HashMap<>();
    if (logicalPlanFacet != null) {
      additionalFacets.put("spark.logicalPlan", logicalPlanFacet);
    }
    if (jobError != null) {
      additionalFacets.put("spark.exception", jobError);
    }
    return RunFacet.builder().parent(parentRunFacet).additional(additionalFacets).build();
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

  protected LineageEvent.Job buildJob(QueryExecution queryExecution) {
    SparkContext sparkContext = queryExecution.executedPlan().sparkContext();
    SparkPlan node = queryExecution.executedPlan();

    // Unwrap SparkPlan from WholeStageCodegen, as that's not a descriptive or helpful job name
    if (node instanceof WholeStageCodegenExec) {
      node = ((WholeStageCodegenExec) node).child();
    }
    return Job.builder()
        .namespace(marquezContext.getJobNamespace())
        .name(
            sparkContext.appName().replaceAll(CAMEL_TO_SNAKE_CASE, "_$1").toLowerCase(Locale.ROOT)
                + "."
                + node.nodeName().replaceAll(CAMEL_TO_SNAKE_CASE, "_$1").toLowerCase(Locale.ROOT))
        .build();
  }
}
