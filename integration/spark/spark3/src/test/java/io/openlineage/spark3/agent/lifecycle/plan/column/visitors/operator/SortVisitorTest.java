/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.column.visitors.operator;

import static io.openlineage.spark3.agent.lifecycle.plan.column.ColumnLevelFixtures.EXPR_ID_1;
import static io.openlineage.spark3.agent.lifecycle.plan.column.ColumnLevelFixtures.NAME_1;
import static io.openlineage.spark3.agent.lifecycle.plan.column.ColumnLevelFixtures.asSeq;
import static io.openlineage.spark3.agent.lifecycle.plan.column.ColumnLevelFixtures.field;
import static io.openlineage.spark3.agent.lifecycle.plan.column.ColumnLevelFixtures.mockNewExprId;
import static io.openlineage.spark3.agent.lifecycle.plan.column.ColumnLevelFixtures.sortOrder;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import io.openlineage.client.utils.TransformationInfo;
import io.openlineage.spark.agent.lifecycle.plan.column.ColumnLevelLineageBuilder;
import java.util.concurrent.atomic.LongAccumulator;
import org.apache.spark.sql.catalyst.expressions.ExprId;
import org.apache.spark.sql.catalyst.expressions.NamedExpression;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.Sort;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

class SortVisitorTest {

  SortVisitor visitor = new SortVisitor();
  ColumnLevelLineageBuilder builder = mock(ColumnLevelLineageBuilder.class);
  LongAccumulator exprIdAccumulator = new LongAccumulator(Long::sum, 0L);

  @BeforeEach
  void setup() {
    exprIdAccumulator.reset();
  }

  @Test
  void testIsDefinedAt() {
    assertTrue(visitor.isDefinedAt(mock(Sort.class)));
    assertFalse(visitor.isDefinedAt(mock(LogicalPlan.class)));
  }

  @Test
  void testApply() {
    try (MockedStatic<NamedExpression> utilities = mockStatic(NamedExpression.class)) {
      mockNewExprId(exprIdAccumulator, utilities);
      ExprId datasetDependencyExpression = ExprId.apply(0);
      Sort sort =
          new Sort(asSeq(sortOrder(field(NAME_1, EXPR_ID_1))), true, mock(LogicalPlan.class));

      visitor.apply(sort, builder);

      verify(builder, times(1)).addDatasetDependency(datasetDependencyExpression);
      verify(builder, times(1))
          .addDependency(
              datasetDependencyExpression,
              EXPR_ID_1,
              TransformationInfo.indirect(TransformationInfo.Subtypes.SORT));
      utilities.verify(NamedExpression::newExprId, times(1));
    }
  }
}
