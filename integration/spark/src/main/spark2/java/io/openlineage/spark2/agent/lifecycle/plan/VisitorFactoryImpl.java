package io.openlineage.spark2.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.lifecycle.plan.VisitorFactory;
import java.util.List;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import scala.runtime.AbstractPartialFunction;

class VisitorFactoryImpl implements VisitorFactory {

  @Override
  public List<AbstractPartialFunction<LogicalPlan, List<OpenLineage.Dataset>>> getInputVisitors() {
    return null;
  }

  @Override
  public List<AbstractPartialFunction<LogicalPlan, List<OpenLineage.Dataset>>> getOutputVisitors() {
    return null;
  }
}
