package openlineage.spark.agent.lifecycle.plan;

import java.util.List;
import openlineage.spark.agent.client.LineageEvent;
import org.apache.spark.sql.catalyst.plans.logical.AppendData;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import scala.PartialFunction;
import scala.runtime.AbstractPartialFunction;

/**
 * {@link LogicalPlan} visitor that matches an {@link AppendData} commands and extracts the output
 * {@link LineageEvent.Dataset} being written.
 */
public class AppendDataVisitor extends AbstractPartialFunction<LogicalPlan, List<LineageEvent.Dataset>> {
  private final List<PartialFunction<LogicalPlan, List<LineageEvent.Dataset>>> outputVisitors;

  public AppendDataVisitor(List<PartialFunction<LogicalPlan, List<LineageEvent.Dataset>>> outputVisitors) {
    this.outputVisitors = outputVisitors;
  }

  @Override
  public boolean isDefinedAt(LogicalPlan x) {
    return x instanceof AppendData;
  }

  @Override
  public List<LineageEvent.Dataset> apply(LogicalPlan x) {
    return PlanUtils.applyFirst(outputVisitors, (LogicalPlan) ((AppendData) x).table());
  }
}
