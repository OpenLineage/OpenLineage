/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.column.visitors.operator;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import io.openlineage.client.utils.TransformationInfo;
import io.openlineage.spark.agent.lifecycle.plan.column.ColumnLevelLineageBuilder;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import java.util.Arrays;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.expressions.AttributeReference;
import org.apache.spark.sql.catalyst.expressions.ExprId;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.plans.logical.Expand;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.types.IntegerType$;
import org.apache.spark.sql.types.Metadata$;
import org.junit.jupiter.api.Test;

class ExpandVisitorTest {

  ExpandVisitor visitor = new ExpandVisitor();
  ColumnLevelLineageBuilder builder = mock(ColumnLevelLineageBuilder.class);

  ExprId inputExprId1 = mock(ExprId.class);
  ExprId inputExprId2 = mock(ExprId.class);
  ExprId outputExprId1 = mock(ExprId.class);
  ExprId outputExprId2 = mock(ExprId.class);

  AttributeReference inputAttr1 =
      new AttributeReference(
          "a", IntegerType$.MODULE$, false, Metadata$.MODULE$.empty(), inputExprId1, null);
  AttributeReference inputAttr2 =
      new AttributeReference(
          "b", IntegerType$.MODULE$, false, Metadata$.MODULE$.empty(), inputExprId2, null);

  AttributeReference outputAttr1 =
      new AttributeReference(
          "a", IntegerType$.MODULE$, false, Metadata$.MODULE$.empty(), outputExprId1, null);
  AttributeReference outputAttr2 =
      new AttributeReference(
          "b", IntegerType$.MODULE$, false, Metadata$.MODULE$.empty(), outputExprId2, null);

  @Test
  void testIsDefinedAt() {
    assertTrue(visitor.isDefinedAt(mock(Expand.class)));
    assertFalse(visitor.isDefinedAt(mock(LogicalPlan.class)));
  }

  @Test
  void testApply() {
    // Simulate simplified Expand with projections:
    // [[a, b], [a, b]]  (two projection rows, same expressions)
    // Output: [a_out, b_out]

    scala.collection.Seq<scala.collection.Seq<Expression>> projections =
        ScalaConversionUtils.<scala.collection.Seq<Expression>>fromList(
                Arrays.asList(
                    ScalaConversionUtils.<Expression>fromList(
                            Arrays.asList((Expression) inputAttr1, (Expression) inputAttr2))
                        .toSeq(),
                    ScalaConversionUtils.<Expression>fromList(
                            Arrays.asList((Expression) inputAttr1, (Expression) inputAttr2))
                        .toSeq()))
            .toSeq();

    scala.collection.Seq<Attribute> output =
        ScalaConversionUtils.<Attribute>fromList(Arrays.asList(outputAttr1, outputAttr2)).toSeq();

    Expand expand = new Expand(projections, output, mock(LogicalPlan.class));

    visitor.apply(expand, builder);

    // Output column 'a' (position 0) should depend on inputAttr1 (appears in both projection rows)
    verify(builder, times(2))
        .addDependency(outputExprId1, inputExprId1, TransformationInfo.identity());

    // Output column 'b' (position 1) should depend on inputAttr2 (appears in both projection rows)
    verify(builder, times(2))
        .addDependency(outputExprId2, inputExprId2, TransformationInfo.identity());
  }
}
