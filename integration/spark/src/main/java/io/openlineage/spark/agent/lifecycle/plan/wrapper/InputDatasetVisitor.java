package io.openlineage.spark.agent.lifecycle.plan.wrapper;

import io.openlineage.client.OpenLineage;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import scala.PartialFunction;
import scala.runtime.AbstractPartialFunction;

/**
 * Wrapper around {@link LogicalPlan} visitors that converts found {@link
 * io.openlineage.client.OpenLineage.Dataset}s into {@link
 * io.openlineage.client.OpenLineage.InputDataset}s and may apply input-specific facets to the
 * returned {@link io.openlineage.client.OpenLineage.InputDataset}.
 */
public class InputDatasetVisitor
    extends AbstractPartialFunction<LogicalPlan, List<OpenLineage.InputDataset>> {

  private final PartialFunction<LogicalPlan, List<OpenLineage.Dataset>> visitor;

  public InputDatasetVisitor(PartialFunction<LogicalPlan, List<OpenLineage.Dataset>> visitor) {
    this.visitor = visitor;
  }

  @Override
  public boolean isDefinedAt(LogicalPlan logicalPlan) {
    return visitor.isDefinedAt(logicalPlan);
  }

  @Override
  public List<OpenLineage.InputDataset> apply(LogicalPlan x) {
    return visitor.apply(x).stream()
        .map(
            dataset ->
                new OpenLineage.InputDatasetBuilder()
                    .name(dataset.getName())
                    .facets(dataset.getFacets())
                    .namespace(dataset.getNamespace())
                    .build())
        .collect(Collectors.toList());
  }
}
