/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.column.visitors.expression;

import static io.openlineage.spark3.agent.lifecycle.plan.column.ColumnLevelFixtures.EXPR_ID_1;
import static io.openlineage.spark3.agent.lifecycle.plan.column.ColumnLevelFixtures.EXPR_ID_2;
import static io.openlineage.spark3.agent.lifecycle.plan.column.ColumnLevelFixtures.EXPR_ID_3;
import static io.openlineage.spark3.agent.lifecycle.plan.column.ColumnLevelFixtures.NAME_1;
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
import org.apache.commons.lang3.reflect.MethodUtils;
import org.apache.spark.sql.catalyst.expressions.AttributeReference;
import org.apache.spark.sql.catalyst.expressions.ExprId;
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression;
import org.apache.spark.sql.catalyst.expressions.aggregate.Complete$;
import org.apache.spark.sql.catalyst.expressions.aggregate.Count;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import scala.Option;
import scala.collection.immutable.Seq;

class AggregateExpressionVisitorTest {

  private final AggregateExpressionVisitor visitor = new AggregateExpressionVisitor();
  private final ExpressionTraverser traverser = mock(ExpressionTraverser.class);
  private final ExpressionTraverser innerTraverser = mock(ExpressionTraverser.class);

  @Test
  void testIsDefinedAt() {
    assertTrue(visitor.isDefinedAt(mock(AggregateExpression.class)));
    assertFalse(visitor.isDefinedAt(mock(AttributeReference.class)));
  }

  @Test
  void testApplyWithResultIdMethod() {
    Count aggFunc = new Count(asSeq(field(NAME_1, EXPR_ID_1)));
    AggregateExpression expr = aggregateExpression(aggFunc, EXPR_ID_2);
    when(traverser.copyFor(eq(aggFunc), any())).thenReturn(innerTraverser);

    visitor.apply(expr, traverser);

    verify(traverser).addDependency(EXPR_ID_2, TransformationInfo.aggregation());
    verify(innerTraverser).traverse();
  }

  @Test
  void testApplyWithResultIdsSequence() {
    Count aggFunc = new Count(asSeq(field(NAME_1, EXPR_ID_1)));
    AggregateExpression expr = aggregateExpression(aggFunc, EXPR_ID_2);
    when(traverser.copyFor(eq(aggFunc), any())).thenReturn(innerTraverser);

    try (MockedStatic<MethodUtils> utils = Mockito.mockStatic(MethodUtils.class)) {
      simulateRuntimeWithResultIds(utils, expr, asSeq(EXPR_ID_2, EXPR_ID_3));

      visitor.apply(expr, traverser);

      verify(traverser).addDependency(EXPR_ID_2, TransformationInfo.aggregation());
      verify(traverser).addDependency(EXPR_ID_3, TransformationInfo.aggregation());
      verify(innerTraverser).traverse();
    }
  }

  private static void simulateRuntimeWithResultIds(
      MockedStatic<MethodUtils> utils, AggregateExpression expr, Seq<ExprId> resultIds) {
    utils
        .when(() -> MethodUtils.getAccessibleMethod(AggregateExpression.class, "resultId"))
        .thenReturn(null);
    utils.when(() -> MethodUtils.invokeMethod(expr, "resultIds")).thenReturn(resultIds);
  }

  private static AggregateExpression aggregateExpression(Count aggFunc, ExprId resultId) {
    return new AggregateExpression(aggFunc, Complete$.MODULE$, false, Option.empty(), resultId);
  }
}
