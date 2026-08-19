/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.column.visitors.expression;

import static io.openlineage.spark3.agent.lifecycle.plan.column.ColumnLevelFixtures.EXPR_ID_1;
import static io.openlineage.spark3.agent.lifecycle.plan.column.ColumnLevelFixtures.EXPR_ID_2;
import static io.openlineage.spark3.agent.lifecycle.plan.column.ColumnLevelFixtures.NAME_1;
import static io.openlineage.spark3.agent.lifecycle.plan.column.ColumnLevelFixtures.NAME_2;
import static io.openlineage.spark3.agent.lifecycle.plan.column.ColumnLevelFixtures.field;
import static io.openlineage.spark3.agent.lifecycle.plan.column.ColumnLevelFixtures.intLiteral;
import static io.openlineage.spark3.agent.lifecycle.plan.column.ColumnLevelFixtures.scalaUdf;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.openlineage.client.utils.TransformationInfo;
import io.openlineage.spark3.agent.lifecycle.plan.column.ExpressionTraverser;
import org.apache.spark.sql.catalyst.expressions.Add;
import org.apache.spark.sql.catalyst.expressions.AttributeReference;
import org.apache.spark.sql.catalyst.expressions.Md5;
import org.apache.spark.sql.catalyst.expressions.ScalaUDF;
import org.junit.jupiter.api.Test;

class UserDefinedExpressionVisitorTest {
  private final UserDefinedExpressionVisitor visitor = new UserDefinedExpressionVisitor();
  private final ExpressionTraverser traverser = mock(ExpressionTraverser.class);
  private final ExpressionTraverser child1Trav = mock(ExpressionTraverser.class);
  private final ExpressionTraverser child2Trav = mock(ExpressionTraverser.class);

  private static final AttributeReference COL_1 = field(NAME_1, EXPR_ID_1);
  private static final AttributeReference COL_2 = field(NAME_2, EXPR_ID_2);

  @Test
  void testIsDefinedAtUserDefinedExpressions() {
    assertTrue(visitor.isDefinedAt(scalaUdf("my_udf", COL_1)));
  }

  @Test
  void testIsNotDefinedAtRegularExpressions() {
    assertFalse(visitor.isDefinedAt(new Add(COL_1, intLiteral(1))));
    assertFalse(visitor.isDefinedAt(new Md5(COL_1)));
    assertFalse(visitor.isDefinedAt(COL_1));
  }

  @Test
  void testUdfDependenciesAreIndirectTransformationsNotDirect() {
    ScalaUDF expr = scalaUdf("my_udf", COL_1);
    when(traverser.copyFor(eq(COL_1), any())).thenReturn(child1Trav);

    visitor.apply(expr, traverser);

    verify(traverser).copyFor(COL_1, indirectTransformation("UDF: my_udf"));
    verify(child1Trav).traverse();
    // the false-confidence DIRECT/TRANSFORMATION edge must no longer be emitted
    verify(traverser, never()).copyFor(COL_1, TransformationInfo.transformation());
    verify(traverser, never()).copyFor(COL_1, TransformationInfo.transformation(true));
    verify(traverser, never()).copyFor(COL_1, TransformationInfo.identity());
    verify(traverser, never()).copyFor(COL_1);
  }

  @Test
  void testEveryUdfArgumentIsRecorded() {
    ScalaUDF expr = scalaUdf("my_udf", COL_1, COL_2);
    when(traverser.copyFor(eq(COL_1), any())).thenReturn(child1Trav);
    when(traverser.copyFor(eq(COL_2), any())).thenReturn(child2Trav);

    visitor.apply(expr, traverser);

    TransformationInfo expected = indirectTransformation("UDF: my_udf");
    verify(traverser).copyFor(COL_1, expected);
    verify(child1Trav).traverse();
    verify(traverser).copyFor(COL_2, expected);
    verify(child2Trav).traverse();
  }

  @Test
  void testUdfEdgeIsNotMarkedAsMasking() {
    ScalaUDF expr = scalaUdf("my_udf", COL_1);
    when(traverser.copyFor(eq(COL_1), any())).thenReturn(child1Trav);

    visitor.apply(expr, traverser);

    verify(traverser).copyFor(COL_1, indirectTransformation("UDF: my_udf"));
    verify(traverser, never())
        .copyFor(
            COL_1,
            new TransformationInfo(
                TransformationInfo.Types.INDIRECT,
                TransformationInfo.Subtypes.TRANSFORMATION,
                "UDF: my_udf",
                true));
  }

  @Test
  void testAnonymousUdfFallsBackToGenericDescription() {
    ScalaUDF expr = scalaUdf(null, COL_1);
    when(traverser.copyFor(eq(COL_1), any())).thenReturn(child1Trav);

    visitor.apply(expr, traverser);

    verify(traverser).copyFor(COL_1, indirectTransformation("UDF"));
    verify(child1Trav).traverse();
  }

  @Test
  void testUdfWithoutArgumentsProducesNoDependency() {
    ScalaUDF expr = scalaUdf("no_args");

    visitor.apply(expr, traverser);

    verify(traverser, never()).copyFor(any(), any());
    verify(traverser, never()).copyFor(any());
  }

  private static TransformationInfo indirectTransformation(String description) {
    return new TransformationInfo(
        TransformationInfo.Types.INDIRECT,
        TransformationInfo.Subtypes.TRANSFORMATION,
        description,
        false);
  }
}
