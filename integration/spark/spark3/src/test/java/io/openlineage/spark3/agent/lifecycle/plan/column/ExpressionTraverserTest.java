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
import static io.openlineage.spark3.agent.lifecycle.plan.column.ColumnLevelFixtures.scalaUdf;
import static org.mockito.Mockito.never;
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
import org.apache.spark.sql.catalyst.expressions.ScalaUDF;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class ExpressionTraverserTest {
  static final ExprId OUTPUT_EXPRESSION_ID = EXPR_ID_4;
  static final AttributeReference LEAF_NODE_1 = field(NAME_1, EXPR_ID_1);
  static final AttributeReference LEAF_NODE_2 = field(NAME_2, EXPR_ID_2);
  static final AttributeReference LEAF_NODE_3 = field(NAME_3, EXPR_ID_3);

  static final TransformationInfo UDF_TRANSFORMATION =
      new TransformationInfo(
          TransformationInfo.Types.INDIRECT,
          TransformationInfo.Subtypes.TRANSFORMATION,
          "UDF: my_udf",
          false);

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
  void udfDoesNotFallBackToDirectTransformation() {
    ScalaUDF udf = scalaUdf("my_udf", LEAF_NODE_1);

    aTraverser(udf, OUTPUT_EXPRESSION_ID).traverse();

    verify(builder).addDependency(OUTPUT_EXPRESSION_ID, EXPR_ID_1, UDF_TRANSFORMATION);
    verify(builder, never())
        .addDependency(OUTPUT_EXPRESSION_ID, EXPR_ID_1, TransformationInfo.transformation());
    verify(builder, never())
        .addDependency(OUTPUT_EXPRESSION_ID, EXPR_ID_1, TransformationInfo.transformation(true));
    verify(builder, never())
        .addDependency(OUTPUT_EXPRESSION_ID, EXPR_ID_1, TransformationInfo.identity());
  }

  @Test
  void udfNestedInAnExpressionTreeKeepsIndirectTransformation() {
    ScalaUDF udf = scalaUdf("my_udf", LEAF_NODE_1);
    Alias res = alias(new Add(udf, intLiteral(1))).as("a", OUTPUT_EXPRESSION_ID);

    aTraverser(res, OUTPUT_EXPRESSION_ID).traverse();

    // TransformationInfo.merge keeps INDIRECT once it is reached, so a UDF nested under a
    // regular expression still yields an indirect edge rather than a direct one
    verify(builder).addDependency(OUTPUT_EXPRESSION_ID, EXPR_ID_1, UDF_TRANSFORMATION);
    verify(builder, never())
        .addDependency(OUTPUT_EXPRESSION_ID, EXPR_ID_1, TransformationInfo.transformation());
  }

  @Test
  void udfArgumentsAreStillTraversedIntoTheirLeaves() {
    // udf(name1 + name2) -> both leaves must show up as indirect UDF dependencies
    ScalaUDF udf = scalaUdf("my_udf", new Add(LEAF_NODE_1, LEAF_NODE_2));

    aTraverser(udf, OUTPUT_EXPRESSION_ID).traverse();

    verify(builder).addDependency(OUTPUT_EXPRESSION_ID, EXPR_ID_1, UDF_TRANSFORMATION);
    verify(builder).addDependency(OUTPUT_EXPRESSION_ID, EXPR_ID_2, UDF_TRANSFORMATION);
  }

  @Test
  void anonymousUdfGetsGenericDescription() {
    ScalaUDF udf = scalaUdf(null, LEAF_NODE_1);

    aTraverser(udf, OUTPUT_EXPRESSION_ID).traverse();

    verify(builder)
        .addDependency(
            OUTPUT_EXPRESSION_ID,
            EXPR_ID_1,
            new TransformationInfo(
                TransformationInfo.Types.INDIRECT,
                TransformationInfo.Subtypes.TRANSFORMATION,
                "UDF",
                false));
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
