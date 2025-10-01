/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.column.visitors.operator;

import static io.openlineage.spark3.agent.lifecycle.plan.column.ExpressionDependencyCollector.collectFromOperator;

import io.openlineage.spark.agent.lifecycle.plan.column.ColumnLevelLineageBuilder;
import org.apache.spark.sql.catalyst.plans.logical.Distinct;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;

/** Extracts expression dependencies from Distinct operator in {@link LogicalPlan}. */
public class DistinctVisitor implements OperatorVisitor {
  @Override
  public boolean isDefinedAt(LogicalPlan operator) {
    return operator instanceof Distinct;
  }

  @Override
  public void apply(LogicalPlan operator, ColumnLevelLineageBuilder builder) {
    collectFromOperator(builder, ((Distinct) operator).child());
  }
}
