/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.column.visitors.operator;

import io.openlineage.spark.agent.lifecycle.plan.column.ColumnLevelLineageBuilder;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;

/**
 * Interface to visit {@link LogicalPlan} operators to collect expression dependencies within the
 * plan.
 */
public interface OperatorVisitor {

  /**
   * Verifies if the visitor should be applied on the operator
   *
   * @param operator
   * @return
   */
  boolean isDefinedAt(LogicalPlan operator);

  /**
   * Applies the visitor and adds extracted dependencies to {@link ColumnLevelLineageBuilder}
   *
   * @param operator
   * @param builder
   */
  void apply(LogicalPlan operator, ColumnLevelLineageBuilder builder);
}
