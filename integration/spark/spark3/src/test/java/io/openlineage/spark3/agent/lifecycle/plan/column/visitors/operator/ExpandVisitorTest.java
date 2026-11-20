/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.column.visitors.operator;

import static io.openlineage.spark3.agent.lifecycle.plan.column.ColumnLevelFixtures.EXPR_ID_1;
import static io.openlineage.spark3.agent.lifecycle.plan.column.ColumnLevelFixtures.EXPR_ID_2;
import static io.openlineage.spark3.agent.lifecycle.plan.column.ColumnLevelFixtures.EXPR_ID_3;
import static io.openlineage.spark3.agent.lifecycle.plan.column.ColumnLevelFixtures.EXPR_ID_4;
import static io.openlineage.spark3.agent.lifecycle.plan.column.ColumnLevelFixtures.NAME_1;
import static io.openlineage.spark3.agent.lifecycle.plan.column.ColumnLevelFixtures.NAME_2;
import static io.openlineage.spark3.agent.lifecycle.plan.column.ColumnLevelFixtures.NAME_3;
import static io.openlineage.spark3.agent.lifecycle.plan.column.ColumnLevelFixtures.asSeq;
import static io.openlineage.spark3.agent.lifecycle.plan.column.ColumnLevelFixtures.field;
import static io.openlineage.spark3.agent.lifecycle.plan.column.ColumnLevelFixtures.intLiteral;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import io.openlineage.client.utils.TransformationInfo;
import io.openlineage.spark.agent.lifecycle.plan.column.ColumnLevelLineageBuilder;
import org.apache.spark.sql.catalyst.expressions.Add;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.expressions.ExprId;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.Literal;
import org.apache.spark.sql.catalyst.plans.logical.Expand;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.types.IntegerType$;
import org.apache.spark.sql.types.StringType$;
import org.junit.jupiter.api.Test;

class ExpandVisitorTest {
  ExpandVisitor visitor = new ExpandVisitor();
  ColumnLevelLineageBuilder builder = mock(ColumnLevelLineageBuilder.class);

  @Test
  void testIsDefinedAt() {
    assertTrue(visitor.isDefinedAt(mock(Expand.class)));
    assertFalse(visitor.isDefinedAt(mock(LogicalPlan.class)));
  }

  @Test
  void testApplyRollup() {
    // GROUP BY ROLLUP (name2): Expand [[name1, name2], [name1, null]], [name1, name3]
    Expression nullLiteral = Literal.create(null, IntegerType$.MODULE$);
    Expand expand =
        new Expand(
            asSeq(
                asSeq((Expression) field(NAME_1, EXPR_ID_1), field(NAME_2, EXPR_ID_2)),
                asSeq((Expression) field(NAME_1, EXPR_ID_1), nullLiteral)),
            asSeq((Attribute) field(NAME_1, EXPR_ID_1), field(NAME_3, EXPR_ID_3)),
            mock(LogicalPlan.class));

    visitor.apply(expand, builder);

    // name1 is passed through unchanged; name3 is the new grouping attribute for name2
    verify(builder)
        .addDependency(EXPR_ID_3, EXPR_ID_2, NAME_3, TransformationInfo.identity(NAME_2));
    verifyNoMoreInteractions(builder);
  }

  @Test
  void testApplyUnpivot() {
    // UNPIVOT of name1 and name2: Expand [['a', name1], ['b', name2]], [name3 (key), value]
    Expression keyA = Literal.create("a", StringType$.MODULE$);
    Expression keyB = Literal.create("b", StringType$.MODULE$);
    Expand expand =
        new Expand(
            asSeq(
                asSeq(keyA, (Expression) field(NAME_1, EXPR_ID_1)),
                asSeq(keyB, (Expression) field(NAME_2, EXPR_ID_2))),
            asSeq((Attribute) field(NAME_3, EXPR_ID_3), field("value", EXPR_ID_4)),
            mock(LogicalPlan.class));

    visitor.apply(expand, builder);

    verify(builder)
        .addDependency(EXPR_ID_4, EXPR_ID_1, "value", TransformationInfo.identity(NAME_1));
    verify(builder)
        .addDependency(EXPR_ID_4, EXPR_ID_2, "value", TransformationInfo.identity(NAME_2));
    verifyNoMoreInteractions(builder);
  }

  @Test
  void testApplyTraversesRepeatedExpressionsOnce() {
    // GROUP BY CUBE (name1, name2): every grouping column is repeated in half of the projections
    Expression nullLiteral = Literal.create(null, IntegerType$.MODULE$);
    Expand expand =
        new Expand(
            asSeq(
                asSeq(
                    (Expression) field(NAME_1, EXPR_ID_1), field(NAME_2, EXPR_ID_2), intLiteral(0)),
                asSeq((Expression) field(NAME_1, EXPR_ID_1), nullLiteral, intLiteral(1)),
                asSeq(nullLiteral, (Expression) field(NAME_2, EXPR_ID_2), intLiteral(2)),
                asSeq(nullLiteral, nullLiteral, intLiteral(3))),
            asSeq(
                (Attribute) field(NAME_3, EXPR_ID_3),
                field("name4", EXPR_ID_4),
                field("spark_grouping_id", ExprId.apply(25))),
            mock(LogicalPlan.class));

    visitor.apply(expand, builder);

    verify(builder)
        .addDependency(EXPR_ID_3, EXPR_ID_1, NAME_3, TransformationInfo.identity(NAME_1));
    verify(builder)
        .addDependency(EXPR_ID_4, EXPR_ID_2, "name4", TransformationInfo.identity(NAME_2));
    verifyNoMoreInteractions(builder);
  }

  @Test
  void testApplyExpressionInProjection() {
    // Expand [[(name1 + name2)], [(name1 + name2)], [name2]], [name3]
    Expression sum = new Add(field(NAME_1, EXPR_ID_1), field(NAME_2, EXPR_ID_2), false);
    Expand expand =
        new Expand(
            asSeq(asSeq(sum), asSeq(sum), asSeq((Expression) field(NAME_2, EXPR_ID_2))),
            asSeq((Attribute) field(NAME_3, EXPR_ID_3)),
            mock(LogicalPlan.class));

    visitor.apply(expand, builder);

    verify(builder)
        .addDependency(
            EXPR_ID_3, EXPR_ID_1, NAME_3, TransformationInfo.transformation("(name1 + name2)"));
    verify(builder)
        .addDependency(
            EXPR_ID_3, EXPR_ID_2, NAME_3, TransformationInfo.transformation("(name1 + name2)"));
    verify(builder)
        .addDependency(EXPR_ID_3, EXPR_ID_2, NAME_3, TransformationInfo.identity(NAME_2));
    verifyNoMoreInteractions(builder);
  }
}
