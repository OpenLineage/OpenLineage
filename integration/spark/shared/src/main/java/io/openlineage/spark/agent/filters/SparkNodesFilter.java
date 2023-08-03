/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.filters;

import static io.openlineage.spark.agent.filters.EventFilterUtils.getLogicalPlan;

import io.openlineage.spark.api.OpenLineageContext;
import java.util.Arrays;
import java.util.List;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.catalyst.plans.logical.Aggregate;
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation;
import org.apache.spark.sql.catalyst.plans.logical.Project;

/** If a root node of an Spark action is one of defined nodes, we should filter it */
public class SparkNodesFilter implements EventFilter {
  private final OpenLineageContext context;

  private static final List<String> filterNodes =
      Arrays.asList(
          "org.apache.spark.sql.catalyst.plans.logical.ShowTables",
          Project.class.getCanonicalName(),
          Aggregate.class.getCanonicalName(),
          LocalRelation.class.getCanonicalName());

  public SparkNodesFilter(OpenLineageContext context) {
    this.context = context;
  }

  public boolean isDisabled(SparkListenerEvent event) {
    return getLogicalPlan(context)
        .map(plan -> plan.getClass().getCanonicalName())
        .filter(node -> filterNodes.contains(node))
        .isPresent();
  }
}
