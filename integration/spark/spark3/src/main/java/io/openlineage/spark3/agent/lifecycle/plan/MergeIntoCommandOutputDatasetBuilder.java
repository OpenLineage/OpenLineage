/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage.OutputDataset;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.api.AbstractQueryPlanOutputDatasetBuilder;
import io.openlineage.spark.api.OpenLineageContext;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
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

    return delegate(
            context.getOutputDatasetQueryPlanVisitors(), context.getOutputDatasetBuilders(), event)
        .applyOrElse(
            target, ScalaConversionUtils.toScalaFn((lp) -> Collections.<OutputDataset>emptyList()))
        .stream()
        .collect(Collectors.toList());
  }
}
