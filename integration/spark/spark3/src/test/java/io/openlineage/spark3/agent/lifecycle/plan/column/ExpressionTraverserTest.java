/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.column;

import static io.openlineage.spark3.agent.lifecycle.plan.column.ColumnLevelFixtures.AliasBuilder.alias;
import static io.openlineage.spark3.agent.lifecycle.plan.column.ColumnLevelFixtures.EXPR_ID_1;
import static io.openlineage.spark3.agent.lifecycle.plan.column.ColumnLevelFixtures.EXPR_ID_2;
import static io.openlineage.spark3.agent.lifecycle.plan.column.ColumnLevelFixtures.EXPR_ID_3;
import static io.openlineage.spark3.agent.lifecycle.plan.column.ColumnLevelFixtures.EXPR_ID_4;
import static io.openlineage.spark3.agent.lifecycle.plan.column.ColumnLevelFixtures.NAME_1;
import static io.openlineage.spark3.agent.lifecycle.plan.column.ColumnLevelFixtures.NAME_2;
import static io.openlineage.spark3.agent.lifecycle.plan.column.ColumnLevelFixtures.NAME_3;
import static io.openlineage.spark3.agent.lifecycle.plan.column.ColumnLevelFixtures.equalTo;
import static io.openlineage.spark3.agent.lifecycle.plan.column.ColumnLevelFixtures.field;
import static io.openlineage.spark3.agent.lifecycle.plan.column.ColumnLevelFixtures.intLiteral;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;

import io.openlineage.client.utils.TransformationInfo;
import io.openlineage.spark.agent.lifecycle.plan.column.ColumnLevelLineageBuilder;
import org.apache.spark.sql.catalyst.expressions.Add;
import org.apache.spark.sql.catalyst.expressions.Alias;
import org.apache.spark.sql.catalyst.expressions.AttributeReference;
import org.apache.spark.sql.catalyst.expressions.ExprId;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.If;
import org.apache.spark.sql.catalyst.expressions.Md5;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class ExpressionTraverserTest {
  static final ExprId OUTPUT_EXPRESSION_ID = EXPR_ID_4;
  static final AttributeReference LEAF_NODE_1 = field(NAME_1, EXPR_ID_1);
  static final AttributeReference LEAF_NODE_2 = field(NAME_2, EXPR_ID_2);
  static final AttributeReference LEAF_NODE_3 = field(NAME_3, EXPR_ID_3);

  ColumnLevelLineageBuilder builder = Mockito.mock(ColumnLevelLineageBuilder.class);

  @Test
  void transformationDefaultsToIdentity() {
    aTraverser(LEAF_NODE_1, OUTPUT_EXPRESSION_ID).traverse();

    verify(builder)
        .addDependency(
            OUTPUT_EXPRESSION_ID, EXPR_ID_1, "name1", TransformationInfo.identity("name1"));
  }

  @Test
  void copiedTraverserRetainsParameters() {
    ExpressionTraverser t1 =
        aTraverser(LEAF_NODE_1, OUTPUT_EXPRESSION_ID, TransformationInfo.transformation("test"));
    ExpressionTraverser t2 = t1.copyFor(LEAF_NODE_2);

    t1.traverse();
    t2.traverse();

    verify(builder)
        .addDependency(
            eq(OUTPUT_EXPRESSION_ID), eq(EXPR_ID_1), anyString(), any(TransformationInfo.class));
    verify(builder)
        .addDependency(
            eq(OUTPUT_EXPRESSION_ID), eq(EXPR_ID_2), anyString(), any(TransformationInfo.class));
  }

  @Test
  void copiedTraverserHasMergedTransformationInfo() {
    ExpressionTraverser t1 =
        aTraverser(LEAF_NODE_1, OUTPUT_EXPRESSION_ID, TransformationInfo.transformation("test"));
    ExpressionTraverser t2 = t1.copyFor(LEAF_NODE_2, TransformationInfo.identity());

    t2.traverse();

    verify(builder)
        .addDependency(
            eq(OUTPUT_EXPRESSION_ID), eq(EXPR_ID_2), anyString(), any(TransformationInfo.class));
  }

  @Test
  void shouldTraverseTheExpressionTree() {
    If ifExpr =
        new If(equalTo(LEAF_NODE_1, LEAF_NODE_2), LEAF_NODE_3, new Add(LEAF_NODE_3, intLiteral(1)));
    Alias res = alias(ifExpr).as("a", OUTPUT_EXPRESSION_ID);

    aTraverser(res, OUTPUT_EXPRESSION_ID).traverse();

    String description = "(IF((name1 = name2), name3, (name3 + 1))) AS a";
    verify(builder)
        .addDependency(
            OUTPUT_EXPRESSION_ID,
            EXPR_ID_1,
            description,
            TransformationInfo.indirect(TransformationInfo.Subtypes.CONDITIONAL, description));
    verify(builder)
        .addDependency(
            OUTPUT_EXPRESSION_ID,
            EXPR_ID_2,
            description,
            TransformationInfo.indirect(TransformationInfo.Subtypes.CONDITIONAL, description));
    verify(builder, Mockito.times(2))
        .addDependency(
            OUTPUT_EXPRESSION_ID,
            EXPR_ID_3,
            description,
            TransformationInfo.transformation(description));
  }

  @Test
  void traverserFallsBackToGenericHandlingOfExpressions() {
    Add unhandledExpression = new Add(LEAF_NODE_1, intLiteral(1));

    aTraverser(unhandledExpression, OUTPUT_EXPRESSION_ID).traverse();

    verify(builder)
        .addDependency(
            OUTPUT_EXPRESSION_ID,
            EXPR_ID_1,
            "(name1 + 1)",
            TransformationInfo.transformation("(name1 + 1)"));
  }

  @Test
  void handlesMaskingExpressions() {
    Md5 maskingExpression = new Md5(LEAF_NODE_1);

    aTraverser(maskingExpression, OUTPUT_EXPRESSION_ID).traverse();

    verify(builder)
        .addDependency(
            eq(OUTPUT_EXPRESSION_ID), eq(EXPR_ID_1), anyString(), any(TransformationInfo.class));
  }

  @Test
  void addsDependency() {
    aTraverser(LEAF_NODE_1, OUTPUT_EXPRESSION_ID).addDependency(EXPR_ID_1);

    verify(builder)
        .addDependency(
            eq(OUTPUT_EXPRESSION_ID), eq(EXPR_ID_1), anyString(), any(TransformationInfo.class));
  }

  @Test
  void addsDependencyWithMergedTransformationInfo() {
    aTraverser(LEAF_NODE_1, OUTPUT_EXPRESSION_ID, TransformationInfo.transformation("test"))
        .addDependency(EXPR_ID_1, TransformationInfo.identity());

    verify(builder)
        .addDependency(
            eq(OUTPUT_EXPRESSION_ID), eq(EXPR_ID_1), anyString(), any(TransformationInfo.class));
  }

  ExpressionTraverser aTraverser(Expression expression, ExprId outputExpressionId) {
    return ExpressionTraverser.of(expression, outputExpressionId, builder);
  }

  ExpressionTraverser aTraverser(
      Expression expression, ExprId outputExpressionId, TransformationInfo transformationInfo) {
    return ExpressionTraverser.of(
        expression, outputExpressionId, "NOT FINISHED", transformationInfo, builder);
  }
}
