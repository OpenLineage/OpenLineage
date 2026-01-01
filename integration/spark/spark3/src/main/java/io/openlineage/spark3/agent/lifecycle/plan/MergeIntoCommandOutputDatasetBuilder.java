/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage.OutputDataset;
import io.openlineage.spark.api.AbstractQueryPlanOutputDatasetBuilder;
import io.openlineage.spark.api.OpenLineageContext;
import java.util.List;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.SubqueryAlias;
import org.apache.spark.sql.delta.commands.MergeIntoCommand;

@Slf4j
public class MergeIntoCommandOutputDatasetBuilder
    extends AbstractQueryPlanOutputDatasetBuilder<MergeIntoCommand> {

  public MergeIntoCommandOutputDatasetBuilder(OpenLineageContext context) {
    super(context, false);
  }

  @Override
  public boolean isDefinedAtLogicalPlan(LogicalPlan x) {
    return x instanceof MergeIntoCommand;
  }

  @Override
  protected List<OutputDataset> apply(SparkListenerEvent event, MergeIntoCommand x) {
    LogicalPlan target = x.target();
    if (target instanceof SubqueryAlias) {
      target = ((SubqueryAlias) target).child();
    }

    return delegate(target, event);
  }

  @Override
  public Optional<String> jobNameSuffix(MergeIntoCommand x) {
    final LogicalPlan target;
    if (x.target() instanceof SubqueryAlias) {
      target = ((SubqueryAlias) x.target()).child();
    } else {
      target = x.target();
    }

    return context.getOutputDatasetBuilders().stream()
        .filter(b -> b instanceof AbstractQueryPlanOutputDatasetBuilder)
        .map(b -> (AbstractQueryPlanOutputDatasetBuilder) b)
        .map(b -> b.jobNameSuffixFromLogicalPlan(target))
        .filter(Optional::isPresent)
        .map(o -> (String) o.get())
        .findFirst();
  }
}
