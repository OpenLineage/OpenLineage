/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.filters;

import static io.openlineage.spark.agent.filters.EventFilterUtils.getLogicalPlan;

import io.openlineage.spark.api.OpenLineageContext;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.catalyst.plans.logical.Project;

/** If a root node of an Spark action is project, we should filter it */
public class ProjectFilter implements EventFilter {
  private final OpenLineageContext context;

  public ProjectFilter(OpenLineageContext context) {
    this.context = context;
  }

  public boolean isDisabled(SparkListenerEvent event) {
    return getLogicalPlan(context).filter(plan -> plan instanceof Project).isPresent();
  }
}
