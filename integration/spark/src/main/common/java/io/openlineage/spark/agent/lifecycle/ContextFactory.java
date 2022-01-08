package io.openlineage.spark.agent.lifecycle;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.InputDataset;
import io.openlineage.client.OpenLineage.OutputDataset;
import io.openlineage.spark.agent.EventEmitter;
import io.openlineage.spark.agent.client.OpenLineageClient;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.api.OpenLineageContext;
import java.util.List;
import java.util.Optional;
import lombok.AllArgsConstructor;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.QueryExecution;
import org.apache.spark.sql.execution.SQLExecution;
import scala.PartialFunction;

@AllArgsConstructor
public class ContextFactory {

  public final EventEmitter openLineageEventEmitter;

  public void close() {
    openLineageEventEmitter.close();
  }

  public ExecutionContext createRddExecutionContext(int jobId) {
    OpenLineageContext olContext =
        OpenLineageContext.builder()
            .sparkSession(ScalaConversionUtils.asJavaOptional(SparkSession.getActiveSession()))
            .sparkContext(SparkContext.getOrCreate())
            .openLineage(new OpenLineage(OpenLineageClient.OPEN_LINEAGE_CLIENT_URI))
            .build();
    return new RddExecutionContext(olContext, jobId, openLineageEventEmitter);
  }

  public ExecutionContext createSparkSQLExecutionContext(long executionId) {
    QueryExecution queryExecution = SQLExecution.getQueryExecution(executionId);
    SparkSession sparkSession = queryExecution.sparkSession();
    OpenLineageContext olContext =
        OpenLineageContext.builder()
            .sparkSession(Optional.of(sparkSession))
            .sparkContext(sparkSession.sparkContext())
            .openLineage(new OpenLineage(OpenLineageClient.OPEN_LINEAGE_CLIENT_URI))
            .queryExecution(queryExecution)
            .build();

    VisitorFactory visitorFactory = VisitorFactoryProvider.getInstance(sparkSession);

    List<PartialFunction<LogicalPlan, List<InputDataset>>> inputDatasets =
        visitorFactory.getInputVisitors(olContext);
    olContext.getInputDatasetQueryPlanVisitors().addAll(inputDatasets);

    List<PartialFunction<LogicalPlan, List<OutputDataset>>> outputDatasets =
        visitorFactory.getOutputVisitors(olContext);
    olContext.getOutputDatasetQueryPlanVisitors().addAll(outputDatasets);

    return new SparkSQLExecutionContext(
        executionId, openLineageEventEmitter, queryExecution, olContext);
  }
}
