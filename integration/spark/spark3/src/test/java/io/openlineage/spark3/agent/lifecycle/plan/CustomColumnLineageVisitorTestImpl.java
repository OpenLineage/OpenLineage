/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan;

import static org.mockito.Mockito.mock;

import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.spark.agent.lifecycle.plan.column.ColumnLevelLineageBuilder;
import io.openlineage.spark.agent.lifecycle.plan.column.CustomColumnLineageVisitor;
import org.apache.spark.sql.catalyst.expressions.ExprId;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;

public class CustomColumnLineageVisitorTestImpl implements CustomColumnLineageVisitor {
  public static final String OUTPUT_COL_NAME = "outputCol";
  public static final String INPUT_COL_NAME = "inputCol";
  public static LogicalPlan child = mock(LogicalPlan.class);

  public static ExprId childExprId = mock(ExprId.class);
  public static ExprId parentExprId = mock(ExprId.class);

  @Override
  public void collectInputs(LogicalPlan node, ColumnLevelLineageBuilder builder) {
    if (node.equals(child)) {
      builder.addInput(childExprId, mock(DatasetIdentifier.class), INPUT_COL_NAME);
    }
  }

  @Override
  public void collectOutputs(LogicalPlan node, ColumnLevelLineageBuilder builder) {
    if (node.equals(child)) {
      builder.addOutput(parentExprId, OUTPUT_COL_NAME);
    }
  }

  @Override
  public void collectExpressionDependencies(LogicalPlan node, ColumnLevelLineageBuilder builder) {
    if (node.equals(child)) {
      builder.addDependency(parentExprId, childExprId);
    }
  }
}
