/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.column.visitors.expression;

import static io.openlineage.client.utils.TransformationInfo.Subtypes.WINDOW;
import static io.openlineage.spark3.agent.lifecycle.plan.column.ColumnLevelFixtures.EXPR_ID_1;
import static io.openlineage.spark3.agent.lifecycle.plan.column.ColumnLevelFixtures.EXPR_ID_2;
import static io.openlineage.spark3.agent.lifecycle.plan.column.ColumnLevelFixtures.NAME_1;
import static io.openlineage.spark3.agent.lifecycle.plan.column.ColumnLevelFixtures.NAME_2;
import static io.openlineage.spark3.agent.lifecycle.plan.column.ColumnLevelFixtures.asSeq;
import static io.openlineage.spark3.agent.lifecycle.plan.column.ColumnLevelFixtures.field;
import static io.openlineage.spark3.agent.lifecycle.plan.column.ColumnLevelFixtures.intLiteral;
import static io.openlineage.spark3.agent.lifecycle.plan.column.ColumnLevelFixtures.sortOrder;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.openlineage.client.utils.TransformationInfo;
import io.openlineage.spark3.agent.lifecycle.plan.column.ExpressionTraverser;
import org.apache.spark.sql.catalyst.expressions.AttributeReference;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.NthValue;
import org.apache.spark.sql.catalyst.expressions.Rank;
import org.apache.spark.sql.catalyst.expressions.RowFrame$;
import org.apache.spark.sql.catalyst.expressions.RowNumber;
import org.apache.spark.sql.catalyst.expressions.SortOrder;
import org.apache.spark.sql.catalyst.expressions.SpecifiedWindowFrame;
import org.apache.spark.sql.catalyst.expressions.WindowExpression;
import org.apache.spark.sql.catalyst.expressions.WindowSpecDefinition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class WindowVisitorTest {
  private static final WindowVisitor visitor = new WindowVisitor();
  private static ExpressionTraverser traverser;
  private static ExpressionTraverser windowFuncTraverser;
  private static ExpressionTraverser windowSpecTraverser;

  @BeforeEach
  void setUp() {
    traverser = mock(ExpressionTraverser.class);
    windowFuncTraverser = mock(ExpressionTraverser.class);
    windowSpecTraverser = mock(ExpressionTraverser.class);
  }

  @Test
  void testIsDefinedAt() {
    assertTrue(visitor.isDefinedAt(mock(WindowExpression.class)));
    assertFalse(visitor.isDefinedAt(mock(AttributeReference.class)));
  }

  @Test
  void testSkipsTraversingWhenRowNumberLike() {
    WindowSpecDefinition windowSpec = mock(WindowSpecDefinition.class);
    when(windowSpec.children()).thenReturn(asSeq());

    visitor.apply(new WindowExpression(new RowNumber(), windowSpec), traverser);

    verify(traverser, never()).copyFor(any(Expression.class), any());
  }

  @Test
  void testSkipsTraversingWhenRankLike() {
    AttributeReference arg = field(NAME_1, EXPR_ID_1);
    Rank rankFunc = new Rank(asSeq(arg));
    WindowSpecDefinition windowSpec = mock(WindowSpecDefinition.class);
    when(windowSpec.children()).thenReturn(asSeq());

    visitor.apply(new WindowExpression(rankFunc, windowSpec), traverser);

    verify(traverser, never()).copyFor(any(Expression.class), any());
  }

  @Test
  void testApply() {
    AttributeReference partitionBy = field(NAME_1, EXPR_ID_1);
    SortOrder sortOrder = sortOrder(field(NAME_2, EXPR_ID_2));
    SpecifiedWindowFrame frame = frame();
    WindowSpecDefinition spec =
        new WindowSpecDefinition(asSeq(partitionBy), asSeq(sortOrder), frame);
    NthValue windowFunction = new NthValue(field(NAME_2, EXPR_ID_2), intLiteral(1));
    when(traverser.copyFor(any(), any())).thenReturn(windowSpecTraverser);
    when(traverser.copyFor(eq(windowFunction), any())).thenReturn(windowFuncTraverser);

    visitor.apply(new WindowExpression(windowFunction, spec), traverser);

    verify(traverser).copyFor(windowFunction, TransformationInfo.transformation());
    verify(windowFuncTraverser).traverse();

    verify(traverser).copyFor(partitionBy, TransformationInfo.indirect(WINDOW));
    verify(traverser).copyFor(sortOrder, TransformationInfo.indirect(WINDOW));
    verify(traverser).copyFor(frame, TransformationInfo.indirect(WINDOW));
    verify(windowSpecTraverser, times(3)).traverse();
  }

  private static SpecifiedWindowFrame frame() {
    return new SpecifiedWindowFrame(RowFrame$.MODULE$, intLiteral(1), intLiteral(1));
  }
}
