package io.openlineage.spark2.agent.lifecycle.plan;

import com.google.common.collect.ImmutableList;
import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.lifecycle.BaseVisitorFactory;
import io.openlineage.spark.agent.lifecycle.plan.wrapper.OutputDatasetVisitor;
import java.util.List;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import scala.PartialFunction;

public class VisitorFactoryImpl extends BaseVisitorFactory {

  @Override
  public List<PartialFunction<LogicalPlan, List<OpenLineage.OutputDataset>>> getOutputVisitors(
      SQLContext sqlContext, String jobNamespace) {
    return ImmutableList.<PartialFunction<LogicalPlan, List<OpenLineage.OutputDataset>>>builder()
        .addAll(super.getOutputVisitors(sqlContext, jobNamespace))
        .add(new OutputDatasetVisitor(new DatasetSourceVisitor()))
        .build();
  }

  public List<PartialFunction<LogicalPlan, List<OpenLineage.Dataset>>> getCommonVisitors(
      SQLContext sqlContext, String jobNamespace) {
    return ImmutableList.<PartialFunction<LogicalPlan, List<OpenLineage.Dataset>>>builder()
        .addAll(super.getBaseCommonVisitors(sqlContext, jobNamespace))
        .add(new DatasetSourceVisitor())
        .build();
  }
}
