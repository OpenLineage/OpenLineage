package io.openlineage.spark.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.util.PlanUtils;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.QueryPlanVisitor;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.spark.sql.catalyst.plans.logical.InsertIntoDir;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.command.InsertIntoDataSourceDirCommand;
import org.apache.spark.sql.execution.datasources.InsertIntoDataSourceCommand;
import org.apache.spark.sql.execution.datasources.SaveIntoDataSourceCommand;
import scala.PartialFunction;

/**
 * {@link LogicalPlan} visitor that extracts the input query of certain write commands that don't
 * expose their input {@link LogicalPlan#children()}. Plans that expose their children are traversed
 * normally by calling {@link LogicalPlan#collect(PartialFunction)}, but children that aren't
 * exposed get skipped in the collect call, so we need to root them out here.
 */
public class CommandPlanVisitor extends QueryPlanVisitor<LogicalPlan, OpenLineage.InputDataset> {

  public CommandPlanVisitor(OpenLineageContext context) {
    super(context);
  }

  @Override
  public boolean isDefinedAt(LogicalPlan x) {
    return x instanceof SaveIntoDataSourceCommand
        || x instanceof InsertIntoDir
        || x instanceof InsertIntoDataSourceCommand
        || x instanceof InsertIntoDataSourceDirCommand;
  }

  @Override
  public List<OpenLineage.InputDataset> apply(LogicalPlan x) {
    Optional<LogicalPlan> input = getInput(x);
    PartialFunction<LogicalPlan, List<OpenLineage.InputDataset>> inputVisitors =
        PlanUtils.merge(context.getInputDatasetQueryPlanVisitors());
    return input
        .map(
            in ->
                ScalaConversionUtils.fromSeq(in.collect(inputVisitors)).stream()
                    .flatMap(List::stream)
                    .collect(Collectors.toList()))
        .orElseGet(Collections::emptyList);
  }

  private Optional<LogicalPlan> getInput(LogicalPlan x) {
    if (x instanceof SaveIntoDataSourceCommand) {
      return Optional.of(((SaveIntoDataSourceCommand) x).query());
    } else if (x instanceof InsertIntoDir) {
      return Optional.of(((InsertIntoDir) x).child());
    } else if (x instanceof InsertIntoDataSourceCommand) {
      return Optional.of(((InsertIntoDataSourceCommand) x).query());
    } else if (x instanceof InsertIntoDataSourceDirCommand) {
      return Optional.of(((InsertIntoDataSourceDirCommand) x).query());
    }
    return Optional.empty();
  }
}
