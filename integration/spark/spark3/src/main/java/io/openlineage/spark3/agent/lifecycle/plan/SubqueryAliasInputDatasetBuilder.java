/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan;

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
import org.apache.spark.sql.catalyst.plans.logical.SubqueryAlias;

@Slf4j
public class SubqueryAliasInputDatasetBuilder
    extends AbstractQueryPlanInputDatasetBuilder<SubqueryAlias> {

  public SubqueryAliasInputDatasetBuilder(OpenLineageContext context) {
    super(context, true);
  }

  @Override
  public boolean isDefinedAtLogicalPlan(LogicalPlan x) {
    return x instanceof SubqueryAlias && ((SubqueryAlias) x).child() != null;
  }

  @Override
  protected List<InputDataset> apply(SparkListenerEvent event, SubqueryAlias x) {
    return delegate(
            context.getInputDatasetQueryPlanVisitors(), context.getInputDatasetBuilders(), event)
        .applyOrElse(
            x.child(),
            ScalaConversionUtils.toScalaFn((lp) -> Collections.<InputDataset>emptyList()))
        .stream()
        .collect(Collectors.toList());
  }
}
