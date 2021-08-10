package openlineage.spark.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import openlineage.spark.agent.lifecycle.plan.wrapper.InputDatasetVisitor;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import scala.PartialFunction;

/**
 * Constructs a list of valid {@link LogicalPlan} visitors that can extract an input {@link
 * OpenLineage.Dataset}. Checks the classpath for classes that are not bundled with Spark to avoid
 * {@link ClassNotFoundException}s during plan traversal.
 */
public class InputDatasetVisitors
    implements Supplier<List<PartialFunction<LogicalPlan, List<OpenLineage.InputDataset>>>> {
  private Supplier<List<PartialFunction<LogicalPlan, List<OpenLineage.Dataset>>>> commonVisitors;

  public InputDatasetVisitors(CommonDatasetVisitors commonDatasetVisitors) {
    this.commonVisitors = commonDatasetVisitors;
  }

  @Override
  public List<PartialFunction<LogicalPlan, List<OpenLineage.InputDataset>>> get() {
    return commonVisitors.get().stream().map(InputDatasetVisitor::new).collect(Collectors.toList());
  }
}
