/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage.OutputDataset;
import io.openlineage.spark.api.AbstractQueryPlanOutputDatasetBuilder;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark3.agent.utils.CopyIntoCommandUtils;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;

/**
 * Extracts output datasets from the Databricks-specific {@code CopyIntoCommand} or {@code
 * CopyIntoCommandEdge}. Since these classes belong to the Databricks runtime and are not available
 * at compile time, reflection is used to access their target relation.
 */
@Slf4j
public class CopyIntoCommandOutputDatasetBuilder
    extends AbstractQueryPlanOutputDatasetBuilder<LogicalPlan> {

  public CopyIntoCommandOutputDatasetBuilder(OpenLineageContext context) {
    super(context, false);
  }

  @Override
  public boolean isDefinedAtLogicalPlan(LogicalPlan x) {
    return CopyIntoCommandUtils.isCopyIntoCommand(x);
  }

  @Override
  protected List<OutputDataset> apply(SparkListenerEvent event, LogicalPlan x) {
    return CopyIntoCommandUtils.target(x)
        .map(target -> delegate(target, event))
        .orElse(Collections.emptyList());
  }

  @Override
  public Optional<String> jobNameSuffix(LogicalPlan plan) {
    return CopyIntoCommandUtils.target(plan)
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
