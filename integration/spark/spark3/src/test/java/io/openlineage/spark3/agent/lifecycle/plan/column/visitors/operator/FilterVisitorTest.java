/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.column.visitors.operator;

import static io.openlineage.spark3.agent.lifecycle.plan.column.ColumnLevelFixtures.EXPR_ID_1;
import static io.openlineage.spark3.agent.lifecycle.plan.column.ColumnLevelFixtures.EXPR_ID_2;
import static io.openlineage.spark3.agent.lifecycle.plan.column.ColumnLevelFixtures.EXPR_ID_3;
import static io.openlineage.spark3.agent.lifecycle.plan.column.ColumnLevelFixtures.NAME_1;
import static io.openlineage.spark3.agent.lifecycle.plan.column.ColumnLevelFixtures.NAME_2;
import static io.openlineage.spark3.agent.lifecycle.plan.column.ColumnLevelFixtures.NAME_3;
import static io.openlineage.spark3.agent.lifecycle.plan.column.ColumnLevelFixtures.equalTo;
import static io.openlineage.spark3.agent.lifecycle.plan.column.ColumnLevelFixtures.field;
import static io.openlineage.spark3.agent.lifecycle.plan.column.ColumnLevelFixtures.greaterThan;
import static io.openlineage.spark3.agent.lifecycle.plan.column.ColumnLevelFixtures.mockNewExprId;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import io.openlineage.client.utils.TransformationInfo;
import io.openlineage.spark.agent.lifecycle.plan.column.ColumnLevelLineageBuilder;
import java.util.concurrent.atomic.LongAccumulator;
import org.apache.spark.sql.catalyst.expressions.And;
import org.apache.spark.sql.catalyst.expressions.EqualTo;
import org.apache.spark.sql.catalyst.expressions.ExprId;
import org.apache.spark.sql.catalyst.expressions.GreaterThan;
import org.apache.spark.sql.catalyst.expressions.NamedExpression;
import org.apache.spark.sql.catalyst.plans.logical.Filter;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

class FilterVisitorTest {

  FilterVisitor visitor = new FilterVisitor();
  ColumnLevelLineageBuilder builder = mock(ColumnLevelLineageBuilder.class);
  LongAccumulator exprIdAccumulator = new LongAccumulator(Long::sum, 0L);

  @BeforeEach
  void setup() {
    exprIdAccumulator.reset();
  }

  @Test
  void testIsDefinedAt() {
    assertTrue(visitor.isDefinedAt(mock(Filter.class)));
    assertFalse(visitor.isDefinedAt(mock(LogicalPlan.class)));
  }

  @Test
  void testApply() {
    try (MockedStatic<NamedExpression> utilities = mockStatic(NamedExpression.class)) {
      mockNewExprId(exprIdAccumulator, utilities);
      ExprId datasetDependencyExpression = ExprId.apply(0);
      EqualTo equalTo = equalTo(field(NAME_1, EXPR_ID_1), field(NAME_2, EXPR_ID_2));
      GreaterThan greaterThan = greaterThan(field(NAME_3, EXPR_ID_3), 5);
      Filter filter = new Filter(new And(equalTo, greaterThan), mock(LogicalPlan.class));

      visitor.apply(filter, builder);

      verify(builder, times(1)).addDatasetDependency(datasetDependencyExpression);
      verify(builder, times(1))
          .addDependency(
              datasetDependencyExpression,
              EXPR_ID_1,
              TransformationInfo.indirect(TransformationInfo.Subtypes.FILTER));
      verify(builder, times(1))
          .addDependency(
              datasetDependencyExpression,
              EXPR_ID_2,
              TransformationInfo.indirect(TransformationInfo.Subtypes.FILTER));
      verify(builder, times(1))
          .addDependency(
              datasetDependencyExpression,
              EXPR_ID_3,
              TransformationInfo.indirect(TransformationInfo.Subtypes.FILTER));
      utilities.verify(NamedExpression::newExprId, times(1));
    }
  }
}
