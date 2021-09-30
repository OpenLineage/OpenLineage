package io.openlineage.spark.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import java.util.List;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import scala.PartialFunction;

/**
 * Provides Visitors for iterating on {@link LogicalPlan}.
 *
 * <p>All common Visitors should be grouped and passed to {@link
 * VisitorFactory#getOutputVisitors(List)} to retrieve Visitors for {@link
 * OpenLineage.OutputDataset}
 */
public interface VisitorFactory {

  List<PartialFunction<LogicalPlan, List<OpenLineage.Dataset>>> getCommonVisitors();

  List<PartialFunction<LogicalPlan, List<OpenLineage.InputDataset>>> getInputVisitors();

  List<PartialFunction<LogicalPlan, List<OpenLineage.OutputDataset>>> getOutputVisitors(
      List<PartialFunction<LogicalPlan, List<OpenLineage.Dataset>>> commonVisitors);
}
