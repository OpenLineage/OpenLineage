/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage.OutputDataset;
import io.openlineage.spark.api.AbstractQueryPlanOutputDatasetBuilder;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark3.agent.utils.DeleteUpdateCommandUtils;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;

/**
 * Extracts the modified table of a Databricks {@code DELETE FROM} or {@code UPDATE} as an output
 * dataset.
 *
 * <p>{@link TableContentChangeDatasetBuilder} covers the catalyst {@code DeleteFromTable} / {@code
 * UpdateTable} nodes, which Databricks never produces - the runtime resolves the statement to a
 * Delta command instead. Without this builder those statements emit an event with no datasets at
 * all: nothing matches the plan, so the target is missing from {@code outputs} and the job name
 * carries no table suffix either.
 */
@Slf4j
public class DeleteUpdateCommandOutputDatasetBuilder
    extends AbstractQueryPlanOutputDatasetBuilder<LogicalPlan> {

  public DeleteUpdateCommandOutputDatasetBuilder(OpenLineageContext context) {
    super(context, false);
  }

  @Override
  public boolean isDefinedAtLogicalPlan(LogicalPlan x) {
    return DeleteUpdateCommandUtils.isDeleteOrUpdateCommand(x);
  }

  @Override
  protected List<OutputDataset> apply(SparkListenerEvent event, LogicalPlan x) {
    return DeleteUpdateCommandUtils.target(x)
        .map(target -> delegate(target, event))
        .orElse(Collections.emptyList());
  }

  @Override
  public Optional<String> jobNameSuffix(LogicalPlan plan) {
    return DeleteUpdateCommandUtils.target(plan)
        .flatMap(
            target ->
                context.getOutputDatasetBuilders().stream()
                    .filter(b -> b instanceof AbstractQueryPlanOutputDatasetBuilder)
                    .map(b -> (AbstractQueryPlanOutputDatasetBuilder) b)
                    .map(b -> b.jobNameSuffixFromLogicalPlan(target))
                    .filter(Optional::isPresent)
                    .map(o -> (String) o.get())
                    .findFirst());
  }
}
