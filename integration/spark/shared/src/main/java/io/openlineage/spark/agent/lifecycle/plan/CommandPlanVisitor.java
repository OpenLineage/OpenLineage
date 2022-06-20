/* SPDX-License-Identifier: Apache-2.0 */

package io.openlineage.spark.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage.InputDataset;
import io.openlineage.spark.agent.util.PlanUtils;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.api.AbstractQueryPlanDatasetBuilder;
import io.openlineage.spark.api.OpenLineageContext;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.catalyst.plans.logical.InsertIntoDir;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.command.InsertIntoDataSourceDirCommand;
import org.apache.spark.sql.execution.datasources.InsertIntoDataSourceCommand;
import org.apache.spark.sql.execution.datasources.SaveIntoDataSourceCommand;
import scala.Function1;
import scala.PartialFunction;

/**
 * {@link LogicalPlan} visitor that extracts the input query of certain write commands that don't
 * expose their input {@link LogicalPlan#children()}. Plans that expose their children are traversed
 * normally by calling {@link LogicalPlan#collect(PartialFunction)}, but children that aren't
 * exposed get skipped in the collect call, so we need to root them out here.
 */
public class CommandPlanVisitor
    extends AbstractQueryPlanDatasetBuilder<SparkListenerEvent, LogicalPlan, InputDataset> {

  public static final Function1<LogicalPlan, List<InputDataset>> EMPTY_COLLECTION_FN =
      ScalaConversionUtils.toScalaFn(lp -> Collections.emptyList());

  public CommandPlanVisitor(OpenLineageContext context) {
    super(context, true);
  }

  @Override
  public boolean isDefinedAtLogicalPlan(LogicalPlan x) {
    return x instanceof SaveIntoDataSourceCommand
        || x instanceof InsertIntoDir
        || x instanceof InsertIntoDataSourceCommand
        || x instanceof InsertIntoDataSourceDirCommand;
  }

  @Override
  public boolean isDefinedAt(SparkListenerEvent x) {
    return super.isDefinedAt(x)
        && context
            .getQueryExecution()
            .filter(qe -> isDefinedAtLogicalPlan(qe.optimizedPlan()))
            .isPresent();
  }

  @Override
  public List<InputDataset> apply(LogicalPlan x) {
    Optional<LogicalPlan> input = getInput(x);
    PartialFunction<LogicalPlan, Collection<InputDataset>> inputVisitors =
        PlanUtils.merge(context.getInputDatasetQueryPlanVisitors());
    return input
        .map(
            in ->
                ScalaConversionUtils.fromSeq(in.collect(inputVisitors)).stream()
                    .flatMap(Collection::stream)
                    .collect(Collectors.toList()))
        .orElseGet(Collections::emptyList);
  }

  @Override
  public List<InputDataset> apply(SparkListenerEvent event, LogicalPlan plan) {
    Optional<LogicalPlan> input = getInput(plan);
    PartialFunction<LogicalPlan, Collection<InputDataset>> delegateFn =
        delegate(
            context.getInputDatasetQueryPlanVisitors(), context.getInputDatasetBuilders(), event);
    return input.map(in -> in.collect(delegateFn)).map(ScalaConversionUtils::fromSeq)
        .orElse(Collections.emptyList()).stream()
        .flatMap(Collection::stream)
        .collect(Collectors.toList());
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
