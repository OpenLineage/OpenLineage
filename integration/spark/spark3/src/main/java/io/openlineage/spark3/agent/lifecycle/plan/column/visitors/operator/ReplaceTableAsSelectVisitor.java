/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.column.visitors.operator;

import static io.openlineage.spark3.agent.lifecycle.plan.column.ExpressionDependencyCollector.collectFromOperator;

import io.openlineage.spark.agent.lifecycle.plan.column.ColumnLevelLineageBuilder;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.ReplaceTableAsSelect;

/** Extracts expression dependencies from ReplaceTableAsSelect operator in {@link LogicalPlan}. */
public class ReplaceTableAsSelectVisitor implements OperatorVisitor {
  @Override
  public boolean isDefinedAt(LogicalPlan operator) {
    return operator instanceof ReplaceTableAsSelect
        && (operator.children() == null || operator.children().isEmpty());
  }

  @Override
  public void apply(LogicalPlan operator, ColumnLevelLineageBuilder builder) {
    collectFromOperator(builder, ((ReplaceTableAsSelect) operator).query());
  }
}
