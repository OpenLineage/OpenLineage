/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.column.visitors.operator;

import static io.openlineage.spark3.agent.lifecycle.plan.column.ColumnLevelFixtures.EXPR_ID_1;
import static io.openlineage.spark3.agent.lifecycle.plan.column.ColumnLevelFixtures.EXPR_ID_2;
import static io.openlineage.spark3.agent.lifecycle.plan.column.ColumnLevelFixtures.NAME_1;
import static io.openlineage.spark3.agent.lifecycle.plan.column.ColumnLevelFixtures.NAME_2;
import static io.openlineage.spark3.agent.lifecycle.plan.column.ColumnLevelFixtures.equalTo;
import static io.openlineage.spark3.agent.lifecycle.plan.column.ColumnLevelFixtures.field;
import static io.openlineage.spark3.agent.lifecycle.plan.column.ColumnLevelFixtures.mockNewExprId;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import io.openlineage.client.utils.TransformationInfo;
import io.openlineage.spark.agent.lifecycle.plan.column.ColumnLevelLineageBuilder;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import java.util.concurrent.atomic.LongAccumulator;
import org.apache.spark.sql.catalyst.expressions.ExprId;
import org.apache.spark.sql.catalyst.expressions.NamedExpression;
import org.apache.spark.sql.catalyst.plans.JoinType;
import org.apache.spark.sql.catalyst.plans.logical.Join;
import org.apache.spark.sql.catalyst.plans.logical.JoinHint;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import scala.Option;

class JoinVisitorTest {

  JoinVisitor visitor = new JoinVisitor();
  ColumnLevelLineageBuilder builder = mock(ColumnLevelLineageBuilder.class);
  LongAccumulator exprIdAccumulator = new LongAccumulator(Long::sum, 0L);

  @BeforeEach
  void setup() {
    exprIdAccumulator.reset();
  }

  @Test
  void testIsDefinedAt() {
    assertTrue(visitor.isDefinedAt(mock(Join.class)));
    assertFalse(visitor.isDefinedAt(mock(LogicalPlan.class)));
  }

  @Test
  void testApply() {
    try (MockedStatic<NamedExpression> utilities = mockStatic(NamedExpression.class)) {
      mockNewExprId(exprIdAccumulator, utilities);
      ExprId datasetDependencyExpression = ExprId.apply(0);
      Join join =
          new Join(
              mock(LogicalPlan.class),
              mock(LogicalPlan.class),
              JoinType.apply("inner"),
              ScalaConversionUtils.toScalaOption(
                  equalTo(field(NAME_1, EXPR_ID_1), field(NAME_2, EXPR_ID_2))),
              JoinHint.NONE());

      visitor.apply(join, builder);

      verify(builder, times(1)).addDatasetDependency(datasetDependencyExpression);
      verify(builder, times(1))
          .addDependency(
              datasetDependencyExpression,
              EXPR_ID_1,
              TransformationInfo.indirect(TransformationInfo.Subtypes.JOIN));
      verify(builder, times(1))
          .addDependency(
              datasetDependencyExpression,
              EXPR_ID_2,
              TransformationInfo.indirect(TransformationInfo.Subtypes.JOIN));
      utilities.verify(NamedExpression::newExprId, times(1));
    }
  }

  @Test
  void testApplyWithoutCondition() {
    Join join =
        new Join(
            mock(LogicalPlan.class),
            mock(LogicalPlan.class),
            JoinType.apply("inner"),
            Option.empty(),
            JoinHint.NONE());

    visitor.apply(join, builder);

    verifyNoInteractions(builder);
  }
}
