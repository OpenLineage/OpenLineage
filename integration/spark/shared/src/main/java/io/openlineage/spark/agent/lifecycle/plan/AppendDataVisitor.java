/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.util.PlanUtils;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.QueryPlanVisitor;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.sql.catalyst.plans.logical.AppendData;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;

/**
 * {@link LogicalPlan} visitor that matches an {@link AppendData} commands and extracts the output
 * {@link OpenLineage.Dataset} being written.
 */
public class AppendDataVisitor extends QueryPlanVisitor<AppendData, OpenLineage.OutputDataset> {

  public AppendDataVisitor(OpenLineageContext context) {
    super(context);
  }

  @Override
  public List<OpenLineage.OutputDataset> apply(LogicalPlan x) {
    // Needs to cast to logical plan despite IntelliJ claiming otherwise.
    return new ArrayList<>(
        PlanUtils.applyAll(
            context.getOutputDatasetQueryPlanVisitors(), (LogicalPlan) ((AppendData) x).table()));
  }
}
