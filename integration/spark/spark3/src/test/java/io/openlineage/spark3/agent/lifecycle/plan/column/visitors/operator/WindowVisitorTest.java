/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.column.visitors.operator;

import static io.openlineage.spark3.agent.lifecycle.plan.column.ColumnLevelFixtures.AliasBuilder.alias;
import static io.openlineage.spark3.agent.lifecycle.plan.column.ColumnLevelFixtures.EXPR_ID_1;
import static io.openlineage.spark3.agent.lifecycle.plan.column.ColumnLevelFixtures.EXPR_ID_2;
import static io.openlineage.spark3.agent.lifecycle.plan.column.ColumnLevelFixtures.EXPR_ID_3;
import static io.openlineage.spark3.agent.lifecycle.plan.column.ColumnLevelFixtures.NAME_1;
import static io.openlineage.spark3.agent.lifecycle.plan.column.ColumnLevelFixtures.NAME_2;
import static io.openlineage.spark3.agent.lifecycle.plan.column.ColumnLevelFixtures.asSeq;
import static io.openlineage.spark3.agent.lifecycle.plan.column.ColumnLevelFixtures.field;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import io.openlineage.client.utils.TransformationInfo;
import io.openlineage.spark.agent.lifecycle.plan.column.ColumnLevelLineageBuilder;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import org.apache.spark.sql.catalyst.expressions.AttributeReference;
import org.apache.spark.sql.catalyst.expressions.Descending$;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.Rank;
import org.apache.spark.sql.catalyst.expressions.RowFrame$;
import org.apache.spark.sql.catalyst.expressions.SortOrder;
import org.apache.spark.sql.catalyst.expressions.SortOrder$;
import org.apache.spark.sql.catalyst.expressions.SpecifiedWindowFrame$;
import org.apache.spark.sql.catalyst.expressions.WindowExpression;
import org.apache.spark.sql.catalyst.expressions.WindowExpression$;
import org.apache.spark.sql.catalyst.expressions.WindowSpecDefinition;
import org.apache.spark.sql.catalyst.expressions.WindowSpecDefinition$;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.Window;
import org.junit.jupiter.api.Test;
import scala.collection.immutable.Seq;

class WindowVisitorTest {

  WindowVisitor visitor = new WindowVisitor();
  ColumnLevelLineageBuilder builder = mock(ColumnLevelLineageBuilder.class);

  @Test
  void testIsDefinedAt() {
    assertTrue(visitor.isDefinedAt(mock(Window.class)));
    assertFalse(visitor.isDefinedAt(mock(LogicalPlan.class)));
  }

  @Test
  void testApply() {
    Seq<SortOrder> sortOrderSeq = asSeq(sortOrder(field(NAME_1, EXPR_ID_1)));
    Seq<Expression> partitionSeq = asSeq(field(NAME_2, EXPR_ID_2));
    WindowExpression winExpr =
        WindowExpression$.MODULE$.apply(
            new Rank(asSeq(field(NAME_1, EXPR_ID_1))),
            windowSpecDefinition(partitionSeq, sortOrderSeq));

    Window window =
        new Window(
            asSeq(alias(winExpr).as("rank", EXPR_ID_3)),
            partitionSeq,
            sortOrderSeq,
            mock(LogicalPlan.class));

    visitor.apply(window, builder);

    verify(builder, times(0))
        .addDependency(EXPR_ID_3, EXPR_ID_1, TransformationInfo.transformation());
    verify(builder, times(1))
        .addDependency(
            EXPR_ID_3, EXPR_ID_1, TransformationInfo.indirect(TransformationInfo.Subtypes.WINDOW));
    verify(builder, times(1))
        .addDependency(
            EXPR_ID_3, EXPR_ID_2, TransformationInfo.indirect(TransformationInfo.Subtypes.WINDOW));
  }

  private static SortOrder sortOrder(AttributeReference child) {
    return SortOrder$.MODULE$.apply(
        child, Descending$.MODULE$, ScalaConversionUtils.asScalaSeqEmpty());
  }

  private static WindowSpecDefinition windowSpecDefinition(
      Seq<Expression> partitionSeq, Seq<SortOrder> sortOrderSeq) {
    return WindowSpecDefinition$.MODULE$.apply(
        partitionSeq,
        sortOrderSeq,
        SpecifiedWindowFrame$.MODULE$.apply(
            mock(RowFrame$.MODULE$.getClass()), mock(Expression.class), mock(Expression.class)));
  }
}
