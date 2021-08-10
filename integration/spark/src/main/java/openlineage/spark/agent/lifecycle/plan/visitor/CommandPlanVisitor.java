package openlineage.spark.agent.lifecycle.plan.visitor;

import io.openlineage.client.OpenLineage;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import openlineage.spark.agent.lifecycle.plan.PlanUtils;
import openlineage.spark.agent.lifecycle.plan.ScalaConversionUtils;
import org.apache.spark.sql.catalyst.plans.logical.InsertIntoDir;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.command.InsertIntoDataSourceDirCommand;
import org.apache.spark.sql.execution.datasources.InsertIntoDataSourceCommand;
import org.apache.spark.sql.execution.datasources.SaveIntoDataSourceCommand;
import scala.PartialFunction;
import scala.runtime.AbstractPartialFunction;

/**
 * {@link LogicalPlan} visitor that extracts the input query of certain write commands that don't
 * expose their input {@link LogicalPlan#children()}. Plans that expose their children are traversed
 * normally by calling {@link LogicalPlan#collect(PartialFunction)}, but children that aren't
 * exposed get skipped in the collect call, so we need to root them out here.
 */
public class CommandPlanVisitor
    extends AbstractPartialFunction<LogicalPlan, List<OpenLineage.Dataset>> {
  private final PartialFunction<LogicalPlan, List<OpenLineage.Dataset>> inputVisitors;

  public CommandPlanVisitor(
      List<PartialFunction<LogicalPlan, List<OpenLineage.Dataset>>> inputVisitors) {
    this.inputVisitors = PlanUtils.merge(inputVisitors);
  }

  @Override
  public boolean isDefinedAt(LogicalPlan x) {
    return x instanceof SaveIntoDataSourceCommand
        || x instanceof InsertIntoDir
        || x instanceof InsertIntoDataSourceCommand
        || x instanceof InsertIntoDataSourceDirCommand;
  }

  @Override
  public List<OpenLineage.Dataset> apply(LogicalPlan x) {
    Optional<LogicalPlan> input = getInput(x);
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
