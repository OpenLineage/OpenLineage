/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.column.visitors;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import io.openlineage.spark.agent.lifecycle.plan.column.ColumnLevelLineageBuilder;
import io.openlineage.spark.agent.lifecycle.plan.column.TransformationInfo;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import java.util.Arrays;
import java.util.Collections;
import org.apache.spark.sql.catalyst.expressions.AttributeReference;
import org.apache.spark.sql.catalyst.expressions.ExprId;
import org.apache.spark.sql.catalyst.expressions.NamedExpression;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.Project;
import org.apache.spark.sql.catalyst.plans.logical.Union;
import org.apache.spark.sql.types.IntegerType$;
import org.apache.spark.sql.types.Metadata$;
import org.junit.jupiter.api.Test;

class UnionFieldDependencyCollectorTest {

  UnionDependencyVisitor visitor = new UnionDependencyVisitor();
  ColumnLevelLineageBuilder builder = mock(ColumnLevelLineageBuilder.class);

  ExprId exprId1 = mock(ExprId.class);
  ExprId exprId2 = mock(ExprId.class);

  NamedExpression expression1 =
      new AttributeReference(
          "name1", IntegerType$.MODULE$, false, Metadata$.MODULE$.empty(), exprId1, null);
  NamedExpression expression2 =
      new AttributeReference(
          "name2", IntegerType$.MODULE$, false, Metadata$.MODULE$.empty(), exprId2, null);

  @Test
  void testIsDefinedAt() {
    assertTrue(visitor.isDefinedAt(mock(Union.class)));
    assertFalse(visitor.isDefinedAt(mock(LogicalPlan.class)));
  }

  @Test
  void testCollect() {
    Union union =
        new Union(
            ScalaConversionUtils.fromList(
                    Arrays.asList(
                        (LogicalPlan)
                            new Project(
                                ScalaConversionUtils.fromList(
                                    Collections.singletonList(expression1)),
                                mock(LogicalPlan.class)),
                        (LogicalPlan)
                            new Project(
                                ScalaConversionUtils.fromList(
                                    Collections.singletonList(expression2)),
                                mock(LogicalPlan.class))))
                .toSeq(),
            true,
            true);

    visitor.apply(union, builder);

    // first element of a union is treated as an ancestor node
    verify(builder, times(1)).addDependency(exprId1, exprId2, TransformationInfo.identity());
  }
}
