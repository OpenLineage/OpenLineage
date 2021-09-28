package io.openlineage.spark3.agent.lifecycle.plan;

import com.google.common.collect.ImmutableList;
import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.lifecycle.plan.VisitorFactory;
import io.openlineage.spark.agent.lifecycle.plan.wrapper.InputDatasetVisitor;
import io.openlineage.spark.agent.lifecycle.plan.wrapper.OutputDatasetVisitor;
import io.openlineage.spark2.agent.lifecycle.plan.DatasetSourceVisitor;
import java.util.List;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import scala.PartialFunction;

public class VisitorFactoryImpl implements VisitorFactory {

  @Override
  public List<PartialFunction<LogicalPlan, List<OpenLineage.InputDataset>>> getInputVisitors() {
    return ImmutableList.of(new InputDatasetVisitor(new DatasetSourceVisitor()));
  }

  @Override
  public List<PartialFunction<LogicalPlan, List<OpenLineage.OutputDataset>>> getOutputVisitors(
      List<PartialFunction<LogicalPlan, List<OpenLineage.Dataset>>> commonVisitors) {
    return ImmutableList.of(new OutputDatasetVisitor(new DatasetSourceVisitor()));
  }

  @Override
  public List<PartialFunction<LogicalPlan, List<OpenLineage.Dataset>>> getCommonVisitors() {
    return ImmutableList.of(new DatasetSourceVisitor());
  }
}
