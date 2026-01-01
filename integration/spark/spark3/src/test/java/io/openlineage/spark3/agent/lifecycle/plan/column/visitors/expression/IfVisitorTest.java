/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.column.visitors.expression;

import static io.openlineage.client.utils.TransformationInfo.Subtypes.CONDITIONAL;
import static io.openlineage.spark3.agent.lifecycle.plan.column.ColumnLevelFixtures.EXPR_ID_1;
import static io.openlineage.spark3.agent.lifecycle.plan.column.ColumnLevelFixtures.EXPR_ID_2;
import static io.openlineage.spark3.agent.lifecycle.plan.column.ColumnLevelFixtures.EXPR_ID_3;
import static io.openlineage.spark3.agent.lifecycle.plan.column.ColumnLevelFixtures.NAME_1;
import static io.openlineage.spark3.agent.lifecycle.plan.column.ColumnLevelFixtures.NAME_2;
import static io.openlineage.spark3.agent.lifecycle.plan.column.ColumnLevelFixtures.NAME_3;
import static io.openlineage.spark3.agent.lifecycle.plan.column.ColumnLevelFixtures.field;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.openlineage.client.utils.TransformationInfo;
import io.openlineage.spark3.agent.lifecycle.plan.column.ExpressionTraverser;
import org.apache.spark.sql.catalyst.expressions.AttributeReference;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.If;
import org.junit.jupiter.api.Test;

class IfVisitorTest {
  private final IfVisitor visitor = new IfVisitor();
  private final ExpressionTraverser traverser = mock(ExpressionTraverser.class);
  private final ExpressionTraverser predTrav = mock(ExpressionTraverser.class);
  private final ExpressionTraverser trueTrav = mock(ExpressionTraverser.class);
  private final ExpressionTraverser falseTrav = mock(ExpressionTraverser.class);

  @Test
  void testIsDefinedAt() {
    assertTrue(visitor.isDefinedAt(mock(If.class)));
    assertFalse(visitor.isDefinedAt(mock(AttributeReference.class)));
  }

  @Test
  void testApply() {
    Expression predicate = field(NAME_1, EXPR_ID_1);
    Expression trueVal = field(NAME_2, EXPR_ID_2);
    Expression falseVal = field(NAME_3, EXPR_ID_3);
    If expr = new If(predicate, trueVal, falseVal);
    when(traverser.copyFor(eq(predicate), any())).thenReturn(predTrav);
    when(traverser.copyFor(trueVal)).thenReturn(trueTrav);
    when(traverser.copyFor(falseVal)).thenReturn(falseTrav);

    visitor.apply(expr, traverser);

    verify(traverser).copyFor(predicate, TransformationInfo.indirect(CONDITIONAL));
    verify(predTrav).traverse();

    verify(traverser).copyFor(eq(trueVal));
    verify(trueTrav).traverse();

    verify(traverser).copyFor(eq(falseVal));
    verify(falseTrav).traverse();
  }
}
