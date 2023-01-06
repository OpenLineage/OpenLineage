/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.api.AbstractQueryPlanOutputDatasetBuilder;
import io.openlineage.spark.api.OpenLineageContext;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.catalyst.plans.logical.AppendData;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;

/**
 * {@link LogicalPlan} visitor that matches an {@link AppendData} commands and extracts the output
 * {@link OpenLineage.Dataset} being written.
 */
@Slf4j
public class AppendDataDatasetBuilder extends AbstractQueryPlanOutputDatasetBuilder<AppendData> {

  public AppendDataDatasetBuilder(OpenLineageContext context) {
    super(context, false);
  }

  @Override
  public boolean isDefinedAtLogicalPlan(LogicalPlan logicalPlan) {
    return logicalPlan instanceof AppendData;
  }

  @Override
  protected List<OpenLineage.OutputDataset> apply(SparkListenerEvent event, AppendData appendData) {
    LogicalPlan logicalPlan = (LogicalPlan) (appendData).table();

    return delegate(
            context.getOutputDatasetQueryPlanVisitors(), context.getOutputDatasetBuilders(), event)
        .applyOrElse(
            logicalPlan,
            ScalaConversionUtils.toScalaFn(
                (lp) -> Collections.<OpenLineage.OutputDataset>emptyList()))
        .stream()
        .collect(Collectors.toList());
  }
}
