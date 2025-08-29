/*
/* Copyright 2018-2025 contributors to the OpenLineage project
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

    verify(builder).addDependency(OUTPUT_EXPRESSION_ID, EXPR_ID_1, TransformationInfo.identity());
  }

  @Test
  void copiedTraverserRetainsParameters() {
    ExpressionTraverser t1 =
        aTraverser(LEAF_NODE_1, OUTPUT_EXPRESSION_ID, TransformationInfo.transformation());
    ExpressionTraverser t2 = t1.copyFor(LEAF_NODE_2);

    t1.traverse();
    t2.traverse();

    verify(builder)
        .addDependency(OUTPUT_EXPRESSION_ID, EXPR_ID_1, TransformationInfo.transformation());
    verify(builder)
        .addDependency(OUTPUT_EXPRESSION_ID, EXPR_ID_2, TransformationInfo.transformation());
  }

  @Test
  void copiedTraverserHasMergedTransformationInfo() {
    ExpressionTraverser t1 =
        aTraverser(LEAF_NODE_1, OUTPUT_EXPRESSION_ID, TransformationInfo.transformation());
    ExpressionTraverser t2 = t1.copyFor(LEAF_NODE_2, TransformationInfo.identity());

    t2.traverse();

    verify(builder)
        .addDependency(OUTPUT_EXPRESSION_ID, EXPR_ID_2, TransformationInfo.transformation());
  }

  @Test
  void shouldTraverseTheExpressionTree() {
    If ifExpr =
        new If(equalTo(LEAF_NODE_1, LEAF_NODE_2), LEAF_NODE_3, new Add(LEAF_NODE_3, intLiteral(1)));
    Alias res = alias(ifExpr).as("a", OUTPUT_EXPRESSION_ID);

    aTraverser(res, OUTPUT_EXPRESSION_ID).traverse();

    verify(builder)
        .addDependency(
            OUTPUT_EXPRESSION_ID,
            EXPR_ID_1,
            TransformationInfo.indirect(TransformationInfo.Subtypes.CONDITIONAL));
    verify(builder)
        .addDependency(
            OUTPUT_EXPRESSION_ID,
            EXPR_ID_2,
            TransformationInfo.indirect(TransformationInfo.Subtypes.CONDITIONAL));
    verify(builder).addDependency(OUTPUT_EXPRESSION_ID, EXPR_ID_3, TransformationInfo.identity());
    verify(builder)
        .addDependency(OUTPUT_EXPRESSION_ID, EXPR_ID_3, TransformationInfo.transformation());
  }

  @Test
  void traverserFallsBackToGenericHandlingOfExpressions() {
    Add unhandledExpression = new Add(LEAF_NODE_1, intLiteral(1));

    aTraverser(unhandledExpression, OUTPUT_EXPRESSION_ID).traverse();

    verify(builder)
        .addDependency(OUTPUT_EXPRESSION_ID, EXPR_ID_1, TransformationInfo.transformation());
  }

  @Test
  void handlesMaskingExpressions() {
    Md5 maskingExpression = new Md5(LEAF_NODE_1);

    aTraverser(maskingExpression, OUTPUT_EXPRESSION_ID).traverse();

    verify(builder)
        .addDependency(OUTPUT_EXPRESSION_ID, EXPR_ID_1, TransformationInfo.transformation(true));
  }

  @Test
  void addsDependency() {
    aTraverser(LEAF_NODE_1, OUTPUT_EXPRESSION_ID).addDependency(EXPR_ID_1);

    verify(builder).addDependency(OUTPUT_EXPRESSION_ID, EXPR_ID_1, TransformationInfo.identity());
  }

  @Test
  void addsDependencyWithMergedTransformationInfo() {
    aTraverser(LEAF_NODE_1, OUTPUT_EXPRESSION_ID, TransformationInfo.transformation())
        .addDependency(EXPR_ID_1, TransformationInfo.identity());

    verify(builder)
        .addDependency(OUTPUT_EXPRESSION_ID, EXPR_ID_1, TransformationInfo.transformation());
  }

  ExpressionTraverser aTraverser(Expression expression, ExprId outputExpressionId) {
    return ExpressionTraverser.of(expression, outputExpressionId, builder);
  }

  ExpressionTraverser aTraverser(
      Expression expression, ExprId outputExpressionId, TransformationInfo transformationInfo) {
    return ExpressionTraverser.of(expression, outputExpressionId, transformationInfo, builder);
  }
}
