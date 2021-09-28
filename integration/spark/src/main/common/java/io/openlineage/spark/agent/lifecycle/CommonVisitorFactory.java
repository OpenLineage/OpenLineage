package io.openlineage.spark.agent.lifecycle;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.lifecycle.plan.AppendDataVisitor;
import io.openlineage.spark.agent.lifecycle.plan.BigQueryNodeVisitor;
import io.openlineage.spark.agent.lifecycle.plan.CommandPlanVisitor;
import io.openlineage.spark.agent.lifecycle.plan.InsertIntoDataSourceDirVisitor;
import io.openlineage.spark.agent.lifecycle.plan.InsertIntoDataSourceVisitor;
import io.openlineage.spark.agent.lifecycle.plan.InsertIntoDirVisitor;
import io.openlineage.spark.agent.lifecycle.plan.InsertIntoHadoopFsRelationVisitor;
import io.openlineage.spark.agent.lifecycle.plan.InsertIntoHiveDirVisitor;
import io.openlineage.spark.agent.lifecycle.plan.InsertIntoHiveTableVisitor;
import io.openlineage.spark.agent.lifecycle.plan.LogicalRDDVisitor;
import io.openlineage.spark.agent.lifecycle.plan.LogicalRelationVisitor;
import io.openlineage.spark.agent.lifecycle.plan.SaveIntoDataSourceCommandVisitor;
import io.openlineage.spark.agent.lifecycle.plan.VisitorFactory;
import io.openlineage.spark.agent.lifecycle.plan.wrapper.InputDatasetVisitor;
import io.openlineage.spark.agent.lifecycle.plan.wrapper.OutputDatasetVisitor;
import io.openlineage.spark.agent.lifecycle.plan.wrapper.OutputDatasetWithMetadataVisitor;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import scala.PartialFunction;

public class CommonVisitorFactory implements VisitorFactory {
  private final SQLContext sqlContext;
  private String jobNamespace;

  public CommonVisitorFactory(SQLContext sqlContext, String jobNamespace) {
    this.sqlContext = sqlContext;
    this.jobNamespace = jobNamespace;
  }

  @Override
  public List<PartialFunction<LogicalPlan, List<OpenLineage.Dataset>>> getCommonVisitors() {
    List<PartialFunction<LogicalPlan, List<OpenLineage.Dataset>>> list = new ArrayList<>();
    list.add(new LogicalRelationVisitor(sqlContext.sparkContext(), jobNamespace));
    list.add(new LogicalRDDVisitor());
    list.add(new CommandPlanVisitor(new ArrayList<>(list)));
    if (BigQueryNodeVisitor.hasBigQueryClasses()) {
      list.add(new BigQueryNodeVisitor(sqlContext));
    }
    return list;
  }

  @Override
  public List<PartialFunction<LogicalPlan, List<OpenLineage.InputDataset>>> getInputVisitors() {
    return getCommonVisitors().stream().map(InputDatasetVisitor::new).collect(Collectors.toList());
  }

  @Override
  public List<PartialFunction<LogicalPlan, List<OpenLineage.OutputDataset>>> getOutputVisitors(
      List<PartialFunction<LogicalPlan, List<OpenLineage.Dataset>>> allCommonVisitors) {
    List<PartialFunction<LogicalPlan, List<OpenLineage.OutputDataset>>> list = new ArrayList<>();

    list.add(new OutputDatasetWithMetadataVisitor(new InsertIntoDataSourceDirVisitor()));
    list.add(
        new OutputDatasetWithMetadataVisitor(new InsertIntoDataSourceVisitor(allCommonVisitors)));
    list.add(new OutputDatasetWithMetadataVisitor(new InsertIntoHadoopFsRelationVisitor()));
    list.add(
        new OutputDatasetWithMetadataVisitor(
            new SaveIntoDataSourceCommandVisitor(sqlContext, allCommonVisitors)));
    list.add(new OutputDatasetVisitor(new AppendDataVisitor(allCommonVisitors)));
    list.add(new OutputDatasetVisitor(new InsertIntoDirVisitor(sqlContext)));
    list.add(
        new OutputDatasetWithMetadataVisitor(
            new InsertIntoHiveTableVisitor(sqlContext.sparkContext())));
    list.add(new OutputDatasetVisitor(new InsertIntoHiveDirVisitor()));
    return list;
  }
}
