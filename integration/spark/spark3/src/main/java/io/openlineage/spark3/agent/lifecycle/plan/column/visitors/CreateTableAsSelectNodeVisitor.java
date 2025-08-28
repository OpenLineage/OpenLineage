/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.column.visitors;

import static io.openlineage.spark3.agent.lifecycle.plan.column.ExpressionDependencyCollector.collectFromNode;

import io.openlineage.spark.agent.lifecycle.plan.column.ColumnLevelLineageBuilder;
import org.apache.spark.sql.catalyst.plans.logical.CreateTableAsSelect;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;

/** Extracts expression dependencies from CreateTableAsSelect node in {@link LogicalPlan}. */
public class CreateTableAsSelectNodeVisitor implements NodeVisitor {
  @Override
  public boolean isDefinedAt(LogicalPlan plan) {
    return plan instanceof CreateTableAsSelect
        && (plan.children() == null || plan.children().isEmpty());
  }

  @Override
  public void apply(LogicalPlan plan, ColumnLevelLineageBuilder builder) {
    collectFromNode(builder, ((CreateTableAsSelect) plan).query());
  }
}
