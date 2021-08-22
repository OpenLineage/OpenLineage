package io.openlineage.spark.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.lifecycle.plan.wrapper.InputDatasetVisitor;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import scala.PartialFunction;

/**
 * Constructs a list of valid {@link LogicalPlan} visitors that can extract an input {@link
 * OpenLineage.Dataset}. Checks the classpath for classes that are not bundled with Spark to avoid
 * {@link ClassNotFoundException}s during plan traversal.
 */
public class InputDatasetVisitors
    implements Supplier<List<PartialFunction<LogicalPlan, List<OpenLineage.InputDataset>>>> {
  private List<PartialFunction<LogicalPlan, List<OpenLineage.Dataset>>> commonVisitors;

  public InputDatasetVisitors(
      List<PartialFunction<LogicalPlan, List<OpenLineage.Dataset>>> commonDatasetVisitors) {
    this.commonVisitors = commonDatasetVisitors;
  }

  @Override
  public List<PartialFunction<LogicalPlan, List<OpenLineage.InputDataset>>> get() {
    return commonVisitors.stream().map(InputDatasetVisitor::new).collect(Collectors.toList());
  }
}
