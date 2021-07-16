package openlineage.spark.agent.lifecycle.plan;

import java.util.List;
import openlineage.spark.agent.client.LineageEvent.Dataset;
import org.apache.spark.sql.catalyst.plans.logical.AppendData;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import scala.PartialFunction;
import scala.runtime.AbstractPartialFunction;

/**
 * {@link LogicalPlan} visitor that matches an {@link AppendData} commands and extracts the output
 * {@link Dataset} being written.
 */
public class AppendDataVisitor extends AbstractPartialFunction<LogicalPlan, List<Dataset>> {
  private final List<PartialFunction<LogicalPlan, List<Dataset>>> outputVisitors;

  public AppendDataVisitor(List<PartialFunction<LogicalPlan, List<Dataset>>> outputVisitors) {
    this.outputVisitors = outputVisitors;
  }

  @Override
  public boolean isDefinedAt(LogicalPlan x) {
    return x instanceof AppendData;
  }

  @Override
  public List<Dataset> apply(LogicalPlan x) {
    return PlanUtils.applyFirst(outputVisitors, (LogicalPlan) ((AppendData) x).table());
  }
}
