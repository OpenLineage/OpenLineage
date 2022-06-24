/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.column.visitors;

import io.openlineage.spark3.agent.lifecycle.plan.column.ColumnLevelLineageBuilder;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;

/**
 * Interface to visit custom {@link LogicalPlan} nodes to collect expression dependencies within the
 * plan.
 */
public interface ExpressionDependencyVisitor {

  /**
   * Verifies if the visitor should be applied on the plan
   *
   * @param plan
   * @return
   */
  boolean isDefinedAt(LogicalPlan plan);

  /**
   * Applies the visitor and adds extracted dependencies to {@link ColumnLevelLineageBuilder}
   *
   * @param plan
   * @param builder
   */
  void apply(LogicalPlan plan, ColumnLevelLineageBuilder builder);
}
