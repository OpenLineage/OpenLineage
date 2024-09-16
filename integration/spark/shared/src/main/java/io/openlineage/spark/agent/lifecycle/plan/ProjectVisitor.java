/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.api.AbstractQueryPlanInputDatasetBuilder;
import io.openlineage.spark.api.OpenLineageContext;
import java.util.List;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.Project;

public class ProjectVisitor extends AbstractQueryPlanInputDatasetBuilder<Project> {
  public ProjectVisitor(OpenLineageContext context) {
    super(context, true);
  }

  @Override
  public boolean isDefinedAtLogicalPlan(LogicalPlan x) {
    return x instanceof Project && ((Project) x).child() != null;
  }

  @Override
  protected List<OpenLineage.InputDataset> apply(SparkListenerEvent event, Project x) {
    return delegate(x.child(), event);
  }
}
