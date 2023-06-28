/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.InputDataset;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.api.AbstractQueryPlanInputDatasetBuilder;
import io.openlineage.spark.api.OpenLineageContext;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.delta.commands.MergeIntoCommand;

@Slf4j
public class MergeIntoCommandInputDatasetBuilder
    extends AbstractQueryPlanInputDatasetBuilder<MergeIntoCommand> {

  public MergeIntoCommandInputDatasetBuilder(OpenLineageContext context) {
    super(context, false);
  }

  @Override
  public boolean isDefinedAtLogicalPlan(LogicalPlan x) {
    return x instanceof MergeIntoCommand;
  }

  @Override
  protected List<OpenLineage.InputDataset> apply(SparkListenerEvent event, MergeIntoCommand x) {
    List<OpenLineage.InputDataset> datasets =
        delegate(
                context.getInputDatasetQueryPlanVisitors(),
                context.getInputDatasetBuilders(),
                event)
            .applyOrElse(
                x.target(),
                ScalaConversionUtils.toScalaFn((lp) -> Collections.<InputDataset>emptyList()))
            .stream()
            .collect(Collectors.toList());

    datasets.addAll(
        delegate(
                context.getInputDatasetQueryPlanVisitors(),
                context.getInputDatasetBuilders(),
                event)
            .applyOrElse(
                x.source(),
                ScalaConversionUtils.toScalaFn((lp) -> Collections.<InputDataset>emptyList()))
            .stream()
            .collect(Collectors.toList()));

    return datasets;
  }
}
