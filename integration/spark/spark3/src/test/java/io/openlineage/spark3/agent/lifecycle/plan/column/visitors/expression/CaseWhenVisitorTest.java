/*
/* Copyright 2018-2025 contributors to the OpenLineage project
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
import org.apache.spark.sql.catalyst.expressions.CaseWhen;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.junit.jupiter.api.Test;
import scala.Option;
import scala.Tuple2;

class CaseWhenVisitorTest {
  CaseWhenVisitor visitor = new CaseWhenVisitor();
  ExpressionTraverser traverser = mock(ExpressionTraverser.class);
  ExpressionTraverser predTrav = mock(ExpressionTraverser.class);
  ExpressionTraverser thenTrav = mock(ExpressionTraverser.class);
  ExpressionTraverser elseTrav = mock(ExpressionTraverser.class);

  @Test
  void testIsDefinedAt() {
    assertTrue(visitor.isDefinedAt(mock(CaseWhen.class)));
    assertFalse(visitor.isDefinedAt(mock(AttributeReference.class)));
  }

  @Test
  void testApply() {
    Expression condExpr = field(NAME_1, EXPR_ID_1);
    Expression thenExpr = field(NAME_2, EXPR_ID_2);
    Expression elseExpr = field(NAME_3, EXPR_ID_3);
    CaseWhen caseWhen =
        new CaseWhen(asSeq(new Tuple2<>(condExpr, thenExpr)), Option.apply(elseExpr));
    when(traverser.copyFor(eq(condExpr), any())).thenReturn(predTrav);
    when(traverser.copyFor(thenExpr)).thenReturn(thenTrav);
    when(traverser.copyFor(elseExpr)).thenReturn(elseTrav);

    visitor.apply(caseWhen, traverser);

    verify(traverser).copyFor(condExpr, TransformationInfo.indirect(CONDITIONAL));
    verify(predTrav).traverse();

    verify(traverser).copyFor(thenExpr);
    verify(thenTrav).traverse();

    verify(traverser).copyFor(elseExpr);
    verify(elseTrav).traverse();
  }
}
