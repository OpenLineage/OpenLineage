/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle.plan.column;

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;

/** Empty interface left for existing custom visitor implementations. */
@Deprecated
public interface CustomColumnLineageVisitor {
  /**
   * Collect inputs for a given {@link LogicalPlan}. Column level lineage mechanism traverses
   * LogicalPlan on its node. This method will be called for each traversed node. Input information
   * should be put into builder.
   *
   * @param node
   * @param builder
   */
  void collectInputs(LogicalPlan node, ColumnLevelLineageBuilder builder);

  /**
   * Collect outputs for a given {@link LogicalPlan}. Column level lineage mechanism traverses
   * LogicalPlan on its node. This method will be called for each traversed node. Output information
   * should be put into builder.
   *
   * @param node
   * @param builder
   */
  void collectOutputs(LogicalPlan node, ColumnLevelLineageBuilder builder);

  /**
   * Collect expressions for a given {@link LogicalPlan}. Column level lineage mechanism traverses
   * LogicalPlan on its node. This method will be called for each traversed node. Expression
   * dependency information should be put into builder.
   *
   * @param node
   * @param builder
   */
  void collectExpressionDependencies(LogicalPlan node, ColumnLevelLineageBuilder builder);
}
