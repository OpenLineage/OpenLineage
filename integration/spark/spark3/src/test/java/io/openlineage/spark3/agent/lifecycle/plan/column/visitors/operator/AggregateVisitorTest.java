/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.column.visitors.operator;

import static io.openlineage.spark3.agent.lifecycle.plan.column.ColumnLevelFixtures.AliasBuilder.alias;
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
import static io.openlineage.spark3.agent.lifecycle.plan.column.ColumnLevelFixtures.mockNewExprId;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import io.openlineage.client.utils.TransformationInfo;
import io.openlineage.spark.agent.lifecycle.plan.column.ColumnLevelLineageBuilder;
import java.util.concurrent.atomic.LongAccumulator;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.expressions.AttributeReference;
import org.apache.spark.sql.catalyst.expressions.ExprId;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.NamedExpression;
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression;
import org.apache.spark.sql.catalyst.expressions.aggregate.Count;
import org.apache.spark.sql.catalyst.plans.logical.Aggregate;
import org.apache.spark.sql.catalyst.plans.logical.Expand;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.Union;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

class AggregateVisitorTest {

  AggregateVisitor visitor = new AggregateVisitor();
  ColumnLevelLineageBuilder builder = mock(ColumnLevelLineageBuilder.class);
  LongAccumulator exprIdAccumulator = new LongAccumulator(Long::sum, 0L);

  @BeforeEach
  void setup() {
    exprIdAccumulator.reset();
  }

  @Test
  void testNotDefinedAtWhenNotAggregate() {
    assertFalse(visitor.isDefinedAt(mock(LogicalPlan.class)));
  }

  @Test
  void testNotDefinedAtWhenChildIsUnionAndThereAreGroupByExprForAllAggregateExpr() {
    Aggregate aggregate =
        new Aggregate(
            asSeq(field(NAME_1, EXPR_ID_1)),
            asSeq(alias(count(field(NAME_2, EXPR_ID_2))).as(NAME_1, EXPR_ID_1)),
            mock(Union.class));

    assertFalse(visitor.isDefinedAt(aggregate));
  }

  @Test
  void testIsDefinedAt() {
    Aggregate aggregate =
        new Aggregate(
            asSeq(field(NAME_1, EXPR_ID_1)),
            asSeq(alias(count(field(NAME_2, EXPR_ID_2))).as(NAME_3, EXPR_ID_3)),
            mock(LogicalPlan.class));

    assertTrue(visitor.isDefinedAt(aggregate));
  }

  @Test
  void testApply() {
    try (MockedStatic<NamedExpression> utilities = mockStatic(NamedExpression.class)) {
      mockNewExprId(exprIdAccumulator, utilities);
      ExprId datasetDependencyExpression = ExprId.apply(0);
      Aggregate aggregate =
          new Aggregate(
              asSeq(field(NAME_1, EXPR_ID_1)),
              asSeq(alias(count(field(NAME_2, EXPR_ID_2))).as(NAME_3, EXPR_ID_3)),
              mock(LogicalPlan.class));

      visitor.apply(aggregate, builder);

      verify(builder, times(1))
          .addDependency(
              EXPR_ID_3,
              EXPR_ID_2,
              "name3",
              TransformationInfo.aggregation("count(name2) AS name3", true));
      verify(builder, times(1)).addDatasetDependency(datasetDependencyExpression, "GROUP BY name1");
      verify(builder, times(1))
          .addDependency(
              datasetDependencyExpression,
              EXPR_ID_1,
              "name1",
              TransformationInfo.indirect(TransformationInfo.Subtypes.GROUP_BY, "name1"));
      utilities.verify(NamedExpression::newExprId, times(1));
    }
  }

  @Test
  void testApplySkipsGroupByOfDistinctAggregatesDeduplication() {
    // SELECT count(DISTINCT a), count(DISTINCT b): Spark groups by [name3, gid] over an Expand
    // only to deduplicate, which is not a GROUP BY of the query
    try (MockedStatic<NamedExpression> utilities = mockStatic(NamedExpression.class)) {
      mockNewExprId(exprIdAccumulator, utilities);
      ExprId groupId = ExprId.apply(25);
      Aggregate aggregate =
          new Aggregate(
              asSeq(field(NAME_3, EXPR_ID_3), field("gid", groupId)),
              asSeq(alias(count(field(NAME_3, EXPR_ID_3))).as("name4", EXPR_ID_4)),
              expand("gid", groupId));

      visitor.apply(aggregate, builder);

      verify(builder, times(1))
          .addDependency(
              EXPR_ID_4,
              EXPR_ID_3,
              "name4",
              TransformationInfo.aggregation("count(name3) AS name4", true));
      verify(builder, never()).addDatasetDependency(any(), any());
      utilities.verify(NamedExpression::newExprId, never());
    }
  }

  @Test
  void testApplyKeepsGroupByOverGroupingSets() {
    // GROUP BY ROLLUP (a): a single Aggregate groups by [name3, spark_grouping_id] over an Expand
    try (MockedStatic<NamedExpression> utilities = mockStatic(NamedExpression.class)) {
      mockNewExprId(exprIdAccumulator, utilities);
      ExprId datasetDependencyExpression = ExprId.apply(0);
      ExprId groupingId = ExprId.apply(25);
      Aggregate aggregate =
          new Aggregate(
              asSeq(field(NAME_3, EXPR_ID_3), field("spark_grouping_id", groupingId)),
              asSeq(alias(count(field(NAME_2, EXPR_ID_2))).as("name4", EXPR_ID_4)),
              expand("spark_grouping_id", groupingId));

      visitor.apply(aggregate, builder);

      verify(builder, times(1))
          .addDatasetDependency(datasetDependencyExpression, "GROUP BY name3, spark_grouping_id");
      verify(builder, times(1))
          .addDependency(
              datasetDependencyExpression,
              EXPR_ID_3,
              "name3",
              TransformationInfo.indirect(TransformationInfo.Subtypes.GROUP_BY, "name3"));
      utilities.verify(NamedExpression::newExprId, times(1));
    }
  }

  /** Expand [[name1, 0], [name1, 1]], [name3, groupIdName] */
  private static Expand expand(String groupIdName, ExprId groupId) {
    return new Expand(
        asSeq(
            asSeq((Expression) field(NAME_1, EXPR_ID_1), intLiteral(0)),
            asSeq((Expression) field(NAME_1, EXPR_ID_1), intLiteral(1))),
        asSeq((Attribute) field(NAME_3, EXPR_ID_3), field(groupIdName, groupId)),
        mock(LogicalPlan.class));
  }

  private static AggregateExpression count(AttributeReference field) {
    return new Count(asSeq(field)).toAggregateExpression();
  }
}
