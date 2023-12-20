/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.column;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.openlineage.spark.agent.lifecycle.plan.column.ColumnLevelLineageBuilder;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.api.OpenLineageContext;
import java.util.Arrays;
import java.util.Collections;
import org.apache.spark.sql.catalyst.expressions.Alias;
import org.apache.spark.sql.catalyst.expressions.AttributeReference;
import org.apache.spark.sql.catalyst.expressions.BinaryExpression;
import org.apache.spark.sql.catalyst.expressions.ExprId;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.NamedExpression;
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression;
import org.apache.spark.sql.catalyst.plans.logical.Aggregate;
import org.apache.spark.sql.catalyst.plans.logical.CreateTableAsSelect;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.Project;
import org.apache.spark.sql.types.IntegerType$;
import org.apache.spark.sql.types.Metadata$;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import scala.Option;
import scala.collection.immutable.Seq;

class ExpressionDependencyCollectorTest {

  ColumnLevelLineageBuilder builder = Mockito.mock(ColumnLevelLineageBuilder.class);
  OpenLineageContext context = mock(OpenLineageContext.class);

  ExprId exprId1 = mock(ExprId.class);
  ExprId exprId2 = mock(ExprId.class);

  ExprId aliasExprId1 = mock(ExprId.class);
  ExprId aliasExprId2 = mock(ExprId.class);

  NamedExpression expression1 =
      new AttributeReference(
          "name1", IntegerType$.MODULE$, false, Metadata$.MODULE$.empty(), exprId1, null);
  NamedExpression expression2 =
      new AttributeReference(
          "name2", IntegerType$.MODULE$, false, Metadata$.MODULE$.empty(), exprId2, null);

  Alias alias1 =
      new Alias(
          (Expression) expression1,
          "name1",
          aliasExprId1,
          ScalaConversionUtils.asScalaSeqEmpty(),
          Option.empty(),
          ScalaConversionUtils.asScalaSeqEmpty());

  Alias alias2 =
      new Alias(
          (Expression) expression2,
          "name2",
          aliasExprId2,
          ScalaConversionUtils.asScalaSeqEmpty(),
          Option.empty(),
          ScalaConversionUtils.asScalaSeqEmpty());

  @Test
  void testCollectFromProjectPlan() {
    Project project =
        new Project(
            ScalaConversionUtils.fromList(
                Arrays.asList((NamedExpression) alias1, (NamedExpression) alias2)),
            mock(LogicalPlan.class));
    LogicalPlan plan = new CreateTableAsSelect(null, null, null, project, null, null, false);

    ExpressionDependencyCollector.collect(context, plan, builder);

    verify(builder, times(1)).addDependency(aliasExprId1, exprId1);
    verify(builder, times(1)).addDependency(aliasExprId2, exprId2);
  }

  @Test
  void testCollectFromAggregatePlan() {
    Aggregate aggregate =
        new Aggregate(
            ScalaConversionUtils.asScalaSeqEmpty(),
            ScalaConversionUtils.fromList(Collections.singletonList((NamedExpression) alias1)),
            mock(LogicalPlan.class));
    LogicalPlan plan = new CreateTableAsSelect(null, null, null, aggregate, null, null, false);

    ExpressionDependencyCollector.collect(context, plan, builder);

    verify(builder, times(1)).addDependency(aliasExprId1, exprId1);
  }

  @Test
  void testCollectTraversingExpressions() {
    // AggregateExpression
    AggregateExpression aggr1 = mock(AggregateExpression.class);
    AggregateExpression aggr2 = mock(AggregateExpression.class);

    when(aggr1.resultId()).thenReturn(exprId1);
    when(aggr2.resultId()).thenReturn(exprId2);

    // BinaryExpression
    Seq<Expression> children =
        ScalaConversionUtils.fromList(Arrays.asList((Expression) aggr1, aggr2)).toSeq();
    BinaryExpression binaryExpression = mock(BinaryExpression.class);
    when(binaryExpression.children()).thenReturn(children);

    ExprId rootAliasExprId = mock(ExprId.class);
    Alias rootAlias =
        new Alias(
            (Expression) binaryExpression,
            "name2",
            rootAliasExprId,
            ScalaConversionUtils.asScalaSeqEmpty(),
            Option.empty(),
            ScalaConversionUtils.asScalaSeqEmpty());

    Project project =
        new Project(
            ScalaConversionUtils.fromList(Collections.singletonList(rootAlias)),
            mock(LogicalPlan.class));
    LogicalPlan plan = new CreateTableAsSelect(null, null, null, project, null, null, false);

    ExpressionDependencyCollector.collect(context, plan, builder);

    verify(builder, times(1)).addDependency(rootAliasExprId, exprId1);
    verify(builder, times(1)).addDependency(rootAliasExprId, exprId2);
  }
}
