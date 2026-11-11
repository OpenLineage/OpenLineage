/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.column.visitors.operator;

import static io.openlineage.spark3.agent.lifecycle.plan.column.ExpressionDependencyCollector.collectFromOperator;

import io.openlineage.spark.agent.lifecycle.plan.column.ColumnLevelLineageBuilder;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.ReplaceTableAsSelect;

/**
 * Extracts expression dependencies from the query of a ReplaceTableAsSelect operator in {@link
 * LogicalPlan} that does not expose the query as a child.
 */
public class ReplaceTableAsSelectVisitor implements OperatorVisitor {
  @Override
  public boolean isDefinedAt(LogicalPlan operator) {
    return operator instanceof ReplaceTableAsSelect
        && (operator.children() == null || operator.children().isEmpty());
  }

  @Override
  public void apply(LogicalPlan operator, ColumnLevelLineageBuilder builder) {
    // The query is not a child of this node, so the regular plan traversal never reaches it.
    // Visit every operator of the query, not only its root.
    ((ReplaceTableAsSelect) operator)
        .query()
        .foreach(
            node -> {
              collectFromOperator(builder, node);
              return scala.runtime.BoxedUnit.UNIT;
            });
  }
}
