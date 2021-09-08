package io.openlineage.spark.agent.lifecycle.plan.wrapper;

import io.openlineage.client.OpenLineage;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.stream.Collectors;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import scala.PartialFunction;
import scala.runtime.AbstractPartialFunction;

/**
 * Wrapper around {@link LogicalPlan} visitors that converts found {@link
 * io.openlineage.client.OpenLineage.Dataset}s into {@link
 * io.openlineage.client.OpenLineage.OutputDataset}s and may apply output-specific facets to the
 * returned {@link io.openlineage.client.OpenLineage.OutputDataset}.
 */
@Slf4j
public class OutputDatasetVisitor
    extends AbstractPartialFunction<LogicalPlan, List<OpenLineage.OutputDataset>> {

  private final PartialFunction<LogicalPlan, List<OpenLineage.Dataset>> visitor;

  public OutputDatasetVisitor(PartialFunction<LogicalPlan, List<OpenLineage.Dataset>> visitor) {
    this.visitor = visitor;
  }

  @Override
  public boolean isDefinedAt(LogicalPlan logicalPlan) {
    return visitor.isDefinedAt(logicalPlan);
  }

  @Override
  public List<OpenLineage.OutputDataset> apply(LogicalPlan x) {
    log.error(this.visitor.getClass().getName());
    return visitor.apply(x).stream()
        .filter(a -> a != null)
        .map(
            dataset ->
                new OpenLineage.OutputDatasetBuilder()
                    .name(dataset.getName())
                    .facets(dataset.getFacets())
                    .namespace(dataset.getNamespace())
                    .build())
        .collect(Collectors.toList());
  }
}
