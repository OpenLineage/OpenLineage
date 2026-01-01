/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle.plan;

import static io.openlineage.client.OpenLineage.OutputDataset;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.util.PlanUtils;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.QueryPlanVisitor;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.apache.spark.sql.catalyst.plans.logical.AppendData;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import scala.PartialFunction;

/**
 * {@link LogicalPlan} visitor that matches an {@link AppendData} commands and extracts the output
 * {@link OpenLineage.Dataset} being written.
 */
public class AppendDataVisitor extends QueryPlanVisitor<AppendData, OutputDataset> {

  public AppendDataVisitor(OpenLineageContext context) {
    super(context);
  }

  @Override
  public List<OutputDataset> apply(LogicalPlan x) {
    PartialFunction<LogicalPlan, Collection<OutputDataset>> fn =
        PlanUtils.merge(context.getOutputDatasetQueryPlanVisitors());
    // Needs to cast to logical plan despite IntelliJ claiming otherwise.
    LogicalPlan table = (LogicalPlan) ((AppendData) x).table();
    if (fn.isDefinedAt(table)) {
      return new ArrayList<>(fn.apply(table));
    }
    return Collections.emptyList();
  }
}
