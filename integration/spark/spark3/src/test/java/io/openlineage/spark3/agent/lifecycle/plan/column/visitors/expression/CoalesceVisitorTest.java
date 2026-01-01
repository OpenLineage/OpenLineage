/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.column.visitors.expression;

import static io.openlineage.client.utils.TransformationInfo.Subtypes.CONDITIONAL;
import static io.openlineage.spark3.agent.lifecycle.plan.column.ColumnLevelFixtures.EXPR_ID_1;
import static io.openlineage.spark3.agent.lifecycle.plan.column.ColumnLevelFixtures.EXPR_ID_2;
import static io.openlineage.spark3.agent.lifecycle.plan.column.ColumnLevelFixtures.NAME_1;
import static io.openlineage.spark3.agent.lifecycle.plan.column.ColumnLevelFixtures.NAME_2;
import static io.openlineage.spark3.agent.lifecycle.plan.column.ColumnLevelFixtures.asSeq;
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
import org.apache.spark.sql.catalyst.expressions.Coalesce;
import org.junit.jupiter.api.Test;

class CoalesceVisitorTest {
  private final CoalesceVisitor visitor = new CoalesceVisitor();
  private final ExpressionTraverser traverser = mock(ExpressionTraverser.class);
  private final ExpressionTraverser cond1Trav = mock(ExpressionTraverser.class);
  private final ExpressionTraverser val1Trav = mock(ExpressionTraverser.class);
  private final ExpressionTraverser cond2Trav = mock(ExpressionTraverser.class);
  private final ExpressionTraverser val2Trav = mock(ExpressionTraverser.class);

  @Test
  void testIsDefinedAt() {
    assertTrue(visitor.isDefinedAt(mock(Coalesce.class)));
    assertFalse(visitor.isDefinedAt(mock(AttributeReference.class)));
  }

  @Test
  void testApply() {
    AttributeReference c1 = field(NAME_1, EXPR_ID_1);
    AttributeReference c2 = field(NAME_2, EXPR_ID_2);
    Coalesce expr = new Coalesce(asSeq(c1, c2));
    when(traverser.copyFor(eq(c1), any())).thenReturn(cond1Trav);
    when(traverser.copyFor(c1)).thenReturn(val1Trav);
    when(traverser.copyFor(eq(c2), any())).thenReturn(cond2Trav);
    when(traverser.copyFor(c2)).thenReturn(val2Trav);

    visitor.apply(expr, traverser);

    verify(traverser).copyFor(c1, TransformationInfo.indirect(CONDITIONAL));
    verify(cond1Trav).traverse();
    verify(traverser).copyFor(c1);
    verify(val1Trav).traverse();

    verify(traverser).copyFor(c2, TransformationInfo.indirect(CONDITIONAL));
    verify(cond2Trav).traverse();
    verify(traverser).copyFor(c2);
    verify(val2Trav).traverse();
  }
}
