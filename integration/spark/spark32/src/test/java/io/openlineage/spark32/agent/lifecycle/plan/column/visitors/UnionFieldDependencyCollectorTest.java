/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark32.agent.lifecycle.plan.column.visitors;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import io.openlineage.spark32.agent.lifecycle.plan.column.ColumnLevelLineageBuilder;
import java.util.Arrays;
import java.util.Collection;
import org.apache.spark.sql.catalyst.expressions.AttributeReference;
import org.apache.spark.sql.catalyst.expressions.ExprId;
import org.apache.spark.sql.catalyst.expressions.NamedExpression;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.Project;
import org.apache.spark.sql.catalyst.plans.logical.Union;
import org.apache.spark.sql.types.IntegerType$;
import org.apache.spark.sql.types.Metadata$;
import org.junit.jupiter.api.Test;
import scala.collection.Seq;

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
            scala.collection.JavaConverters.collectionAsScalaIterableConverter(
                    Arrays.asList(
                        (LogicalPlan)
                            new Project(
                                toScalaSeq(Arrays.asList(expression1)), mock(LogicalPlan.class)),
                        (LogicalPlan)
                            new Project(
                                toScalaSeq(Arrays.asList(expression2)), mock(LogicalPlan.class))))
                .asScala()
                .toSeq(),
            true,
            true);

    visitor.apply(union, builder);

    // first element of a union is treated as an ancestor node
    verify(builder, times(1)).addDependency(exprId1, exprId2);
  }

  private Seq<NamedExpression> toScalaSeq(Collection<NamedExpression> expressions) {
    return scala.collection.JavaConverters.collectionAsScalaIterableConverter(expressions)
        .asScala()
        .toSeq();
  }
}
