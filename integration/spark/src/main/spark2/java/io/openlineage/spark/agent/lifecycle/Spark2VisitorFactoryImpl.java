package io.openlineage.spark.agent.lifecycle;

import com.google.common.collect.ImmutableList;
import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.lifecycle.plan.QueryPlanVisitor;
import io.openlineage.spark.agent.lifecycle.plan.wrapper.OutputDatasetVisitor;
import io.openlineage.spark2.agent.lifecycle.plan.CreateTableLikeCommandVisitor;
import io.openlineage.spark2.agent.lifecycle.plan.DatasetSourceVisitor;
import java.util.List;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;

class Spark2VisitorFactoryImpl extends BaseVisitorFactory {

  @Override
  public List<QueryPlanVisitor<LogicalPlan, OpenLineage.OutputDataset>> getOutputVisitors(
      SQLContext sqlContext, String jobNamespace) {
    return ImmutableList.<QueryPlanVisitor<LogicalPlan, OpenLineage.OutputDataset>>builder()
        .addAll(super.getOutputVisitors(sqlContext, jobNamespace))
        .add(new OutputDatasetVisitor(new DatasetSourceVisitor()))
        .add(new OutputDatasetVisitor(new CreateTableLikeCommandVisitor(sqlContext.sparkSession())))
        .build();
  }

  public List<QueryPlanVisitor<? extends LogicalPlan, OpenLineage.Dataset>> getCommonVisitors(
      SQLContext sqlContext, String jobNamespace) {
    return ImmutableList.<QueryPlanVisitor<? extends LogicalPlan, OpenLineage.Dataset>>builder()
        .addAll(super.getBaseCommonVisitors(sqlContext, jobNamespace))
        .add(new DatasetSourceVisitor())
        .build();
  }
}
