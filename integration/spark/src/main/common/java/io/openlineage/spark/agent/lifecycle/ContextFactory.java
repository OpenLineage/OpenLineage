package io.openlineage.spark.agent.lifecycle;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.OpenLineageContext;
import io.openlineage.spark.agent.lifecycle.plan.BigQueryNodeVisitor;
import io.openlineage.spark.agent.lifecycle.plan.CommandPlanVisitor;
import io.openlineage.spark.agent.lifecycle.plan.LogicalRDDVisitor;
import io.openlineage.spark.agent.lifecycle.plan.LogicalRelationVisitor;
import io.openlineage.spark.agent.lifecycle.plan.VisitorFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.AllArgsConstructor;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.SQLExecution;
import scala.PartialFunction;

@AllArgsConstructor
public class ContextFactory {
  public final OpenLineageContext sparkContext;

  public void close() {
    sparkContext.close();
  }

  public RddExecutionContext createRddExecutionContext(int jobId) {
    return new RddExecutionContext(jobId, sparkContext);
  }

  public SparkSQLExecutionContext createSparkSQLExecutionContext(long executionId) {
    SQLContext sqlContext = SQLExecution.getQueryExecution(executionId).sparkPlan().sqlContext();

    VisitorFactory common = new CommonVisitorFactory(sqlContext, sparkContext.getJobNamespace());
    VisitorFactory versionSpecific =
        VersionSpecificVisitorsProvider.getInstance(SparkSession.active());

    List<PartialFunction<LogicalPlan, List<OpenLineage.Dataset>>> commonDatasets =
        Stream.concat(
                common.getCommonVisitors().stream(), versionSpecific.getCommonVisitors().stream())
            .collect(Collectors.toList());

    List<PartialFunction<LogicalPlan, List<OpenLineage.InputDataset>>> inputDatasets =
        Stream.concat(
                common.getInputVisitors().stream(), versionSpecific.getInputVisitors().stream())
            .collect(Collectors.toList());

    List<PartialFunction<LogicalPlan, List<OpenLineage.OutputDataset>>> outputDatasets =
        Stream.concat(
                common.getOutputVisitors(commonDatasets).stream(),
                versionSpecific.getOutputVisitors(commonDatasets).stream())
            .collect(Collectors.toList());

    return new SparkSQLExecutionContext(executionId, sparkContext, outputDatasets, inputDatasets);
  }

  private List<PartialFunction<LogicalPlan, List<OpenLineage.Dataset>>> commonDatasetVisitors(
      SQLContext sqlContext) {
    List<PartialFunction<LogicalPlan, List<OpenLineage.Dataset>>> list = new ArrayList<>();
    list.add(new LogicalRelationVisitor(sqlContext.sparkContext(), sparkContext.getJobNamespace()));
    list.add(new LogicalRDDVisitor());
    list.add(new CommandPlanVisitor(new ArrayList<>(list)));
    if (BigQueryNodeVisitor.hasBigQueryClasses()) {
      list.add(new BigQueryNodeVisitor(sqlContext));
    }
    return list;
  }
}
