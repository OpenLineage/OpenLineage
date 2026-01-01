/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle.plan.column;

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;

/** Interface for implementing custom collectors of column level lineage. */
public interface ColumnLevelLineageVisitor {

  /**
   * Collect inputs for a given {@link LogicalPlan}. Column level lineage mechanism traverses
   * LogicalPlan on its node. This method will be called for each traversed node. Input information
   * should be put into builder.
   *
   * @param context
   * @param node
   */
  void collectInputs(ColumnLevelLineageContext context, LogicalPlan node);

  /**
   * Collect outputs for a given {@link LogicalPlan}. Column level lineage mechanism traverses
   * LogicalPlan on its node. This method will be called for each traversed node. Output information
   * should be put into builder.
   *
   * @param context
   * @param node
   */
  void collectOutputs(ColumnLevelLineageContext context, LogicalPlan node);

  /**
   * Collect expressions for a given {@link LogicalPlan}. Column level lineage mechanism traverses
   * LogicalPlan on its node. This method will be called for each traversed node. Expression
   * dependency information should be put into builder.
   *
   * @param context
   * @param node
   */
  void collectExpressionDependencies(ColumnLevelLineageContext context, LogicalPlan node);
}
