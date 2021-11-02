package io.openlineage.spark.agent.lifecycle;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.lifecycle.plan.AppendDataVisitor;
import io.openlineage.spark.agent.lifecycle.plan.BigQueryNodeVisitor;
import io.openlineage.spark.agent.lifecycle.plan.CommandPlanVisitor;
import io.openlineage.spark.agent.lifecycle.plan.CreateDataSourceTableAsSelectCommandVisitor;
import io.openlineage.spark.agent.lifecycle.plan.CreateDataSourceTableCommandVisitor;
import io.openlineage.spark.agent.lifecycle.plan.CreateHiveTableAsSelectCommandVisitor;
import io.openlineage.spark.agent.lifecycle.plan.CreateTableLikeCommandVisitor;
import io.openlineage.spark.agent.lifecycle.plan.InsertIntoDataSourceDirVisitor;
import io.openlineage.spark.agent.lifecycle.plan.InsertIntoDataSourceVisitor;
import io.openlineage.spark.agent.lifecycle.plan.InsertIntoDirVisitor;
import io.openlineage.spark.agent.lifecycle.plan.InsertIntoHadoopFsRelationVisitor;
import io.openlineage.spark.agent.lifecycle.plan.InsertIntoHiveDirVisitor;
import io.openlineage.spark.agent.lifecycle.plan.InsertIntoHiveTableVisitor;
import io.openlineage.spark.agent.lifecycle.plan.KafkaRelationVisitor;
import io.openlineage.spark.agent.lifecycle.plan.LoadDataCommandVisitor;
import io.openlineage.spark.agent.lifecycle.plan.LogicalRDDVisitor;
import io.openlineage.spark.agent.lifecycle.plan.LogicalRelationVisitor;
import io.openlineage.spark.agent.lifecycle.plan.QueryPlanVisitor;
import io.openlineage.spark.agent.lifecycle.plan.SaveIntoDataSourceCommandVisitor;
import io.openlineage.spark.agent.lifecycle.plan.wrapper.InputDatasetVisitor;
import io.openlineage.spark.agent.lifecycle.plan.wrapper.OutputDatasetVisitor;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;

abstract class BaseVisitorFactory implements VisitorFactory {

  protected List<QueryPlanVisitor<? extends LogicalPlan, OpenLineage.Dataset>>
      getBaseCommonVisitors(SQLContext sqlContext, String jobNamespace) {
    List<QueryPlanVisitor<? extends LogicalPlan, OpenLineage.Dataset>> list = new ArrayList<>();
    list.add(new LogicalRelationVisitor(sqlContext.sparkContext(), jobNamespace));
    list.add(new LogicalRDDVisitor());
    list.add(new CommandPlanVisitor(new ArrayList<>(list)));
    if (BigQueryNodeVisitor.hasBigQueryClasses()) {
      list.add(new BigQueryNodeVisitor(sqlContext));
    }
    if (KafkaRelationVisitor.hasKafkaClasses()) {
      list.add(new KafkaRelationVisitor());
    }
    return list;
  }

  public abstract List<QueryPlanVisitor<? extends LogicalPlan, OpenLineage.Dataset>>
      getCommonVisitors(SQLContext sqlContext, String jobNamespace);

  @Override
  public List<QueryPlanVisitor<LogicalPlan, OpenLineage.InputDataset>> getInputVisitors(
      SQLContext sqlContext, String jobNamespace) {
    return getCommonVisitors(sqlContext, jobNamespace).stream()
        .map(InputDatasetVisitor::new)
        .collect(Collectors.toList());
  }

  @Override
  public List<QueryPlanVisitor<LogicalPlan, OpenLineage.OutputDataset>> getOutputVisitors(
      SQLContext sqlContext, String jobNamespace) {
    List<QueryPlanVisitor<? extends LogicalPlan, OpenLineage.Dataset>> allCommonVisitors =
        getCommonVisitors(sqlContext, jobNamespace);
    List<QueryPlanVisitor<LogicalPlan, OpenLineage.OutputDataset>> list = new ArrayList<>();

    list.add(new OutputDatasetVisitor(new InsertIntoDataSourceDirVisitor()));
    list.add(new OutputDatasetVisitor(new InsertIntoDataSourceVisitor(allCommonVisitors)));
    list.add(new OutputDatasetVisitor(new InsertIntoHadoopFsRelationVisitor()));
    list.add(
        new OutputDatasetVisitor(
            new SaveIntoDataSourceCommandVisitor(sqlContext, allCommonVisitors)));
    list.add(
        new OutputDatasetWithMetadataVisitor(new CreateDataSourceTableAsSelectCommandVisitor()));
    list.add(new OutputDatasetVisitor(new AppendDataVisitor(allCommonVisitors)));
    list.add(new OutputDatasetVisitor(new InsertIntoDirVisitor(sqlContext)));
    if (InsertIntoHiveTableVisitor.hasHiveClasses()) {
      list.add(new OutputDatasetVisitor(new InsertIntoHiveTableVisitor(sqlContext.sparkContext())));
      list.add(new OutputDatasetVisitor(new InsertIntoHiveDirVisitor()));
      list.add(new OutputDatasetVisitor(new CreateHiveTableAsSelectCommandVisitor()));
    }
    list.add(new OutputDatasetVisitor(new CreateDataSourceTableCommandVisitor()));
    list.add(
        new OutputDatasetVisitor(new CreateTableLikeCommandVisitor(sqlContext.sparkSession())));
    list.add(new OutputDatasetVisitor(new LoadDataCommandVisitor(sqlContext.sparkSession())));
    return list;
  }
}
