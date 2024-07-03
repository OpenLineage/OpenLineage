/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.column;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.openlineage.spark.agent.lifecycle.plan.column.ColumnLevelLineageBuilder;
import io.openlineage.spark.agent.lifecycle.plan.column.ColumnLevelLineageContext;
import io.openlineage.spark.agent.lifecycle.plan.column.TransformationInfo;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.api.OpenLineageContext;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.atomic.LongAccumulator;
import java.util.stream.Collectors;
import org.apache.spark.sql.catalyst.expressions.Add;
import org.apache.spark.sql.catalyst.expressions.Alias;
import org.apache.spark.sql.catalyst.expressions.And;
import org.apache.spark.sql.catalyst.expressions.AttributeReference;
import org.apache.spark.sql.catalyst.expressions.BinaryExpression;
import org.apache.spark.sql.catalyst.expressions.CaseWhen;
import org.apache.spark.sql.catalyst.expressions.EqualTo;
import org.apache.spark.sql.catalyst.expressions.ExprId;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.GreaterThan;
import org.apache.spark.sql.catalyst.expressions.If;
import org.apache.spark.sql.catalyst.expressions.Literal;
import org.apache.spark.sql.catalyst.expressions.NamedExpression;
import org.apache.spark.sql.catalyst.expressions.NullOrdering;
import org.apache.spark.sql.catalyst.expressions.Sha1;
import org.apache.spark.sql.catalyst.expressions.SortDirection;
import org.apache.spark.sql.catalyst.expressions.SortOrder;
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression;
import org.apache.spark.sql.catalyst.expressions.aggregate.Count;
import org.apache.spark.sql.catalyst.plans.JoinType;
import org.apache.spark.sql.catalyst.plans.logical.Aggregate;
import org.apache.spark.sql.catalyst.plans.logical.CreateTableAsSelect;
import org.apache.spark.sql.catalyst.plans.logical.Filter;
import org.apache.spark.sql.catalyst.plans.logical.Join;
import org.apache.spark.sql.catalyst.plans.logical.JoinHint;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.Project;
import org.apache.spark.sql.catalyst.plans.logical.Sort;
import org.apache.spark.sql.types.IntegerType$;
import org.apache.spark.sql.types.Metadata$;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;
import scala.Option;
import scala.collection.immutable.Seq;

class ExpressionDependencyCollectorTest {

  final String NAME1 = "name1";
  final String NAME2 = "name2";
  final String NAME3 = "name3";
  ColumnLevelLineageBuilder builder = Mockito.mock(ColumnLevelLineageBuilder.class);
  ColumnLevelLineageContext context = mock(ColumnLevelLineageContext.class);
  LongAccumulator exprIdAccumulator = new LongAccumulator(Long::sum, 0L);
  ExprId exprId1 = ExprId.apply(21);
  ExprId exprId2 = ExprId.apply(22);
  ExprId exprId3 = ExprId.apply(23);
  ExprId exprId4 = ExprId.apply(24);
  ExprId exprId5 = ExprId.apply(25);

  NamedExpression expression1 = field(NAME1, exprId1);
  NamedExpression expression2 = field(NAME2, exprId2);

  @BeforeEach
  void setup() {
    when(context.getBuilder()).thenReturn(builder);
    when(context.getOlContext()).thenReturn(mock(OpenLineageContext.class));
    exprIdAccumulator.reset();
  }

  @Test
  void testCollectFromProjectPlan() {
    Alias alias = alias(exprId3, NAME2, expression2);
    Alias alias2 =
        alias(exprId4, NAME3, new Add((Expression) expression1, (Expression) expression2));

    Project project =
        new Project(getNamedExpressionSeq(expression1, alias, alias2), mock(LogicalPlan.class));
    LogicalPlan plan = new CreateTableAsSelect(null, null, null, project, null, null, false);

    ExpressionDependencyCollector.collect(context, plan);

    verify(builder, times(1)).addDependency(exprId3, exprId2, TransformationInfo.identity());
    verify(builder, times(1)).addDependency(exprId4, exprId1, TransformationInfo.transformation());
    verify(builder, times(1)).addDependency(exprId4, exprId2, TransformationInfo.transformation());
  }

  @Test
  void testCollectFromAggregatePlan() {
    try (MockedStatic<NamedExpression> utilities = mockStatic(NamedExpression.class)) {
      mockNewExprId(exprIdAccumulator, utilities);
      ExprId datasetDependencyExpression = ExprId.apply(0);
      Seq<Expression> children =
          ScalaConversionUtils.fromList(Collections.singletonList((Expression) expression2));
      Alias alias3 =
          alias(exprId5, NAME3, (Expression) new Count(children).toAggregateExpression());

      Aggregate aggregate =
          new Aggregate(
              getExpressionSeq((Expression) expression1),
              getNamedExpressionSeq(alias3),
              mock(LogicalPlan.class));
      LogicalPlan plan = new CreateTableAsSelect(null, null, null, aggregate, null, null, false);

      ExpressionDependencyCollector.collect(context, plan);

      verify(builder, times(1))
          .addDependency(exprId5, exprId2, TransformationInfo.aggregation(true));
      verify(builder, times(1)).addDatasetDependency(datasetDependencyExpression);
      verify(builder, times(1))
          .addDependency(
              datasetDependencyExpression,
              exprId1,
              TransformationInfo.indirect(TransformationInfo.Subtypes.GROUP_BY));
      utilities.verify(NamedExpression::newExprId, times(1));
    }
  }

  @Test
  void testCollectFromFilterPlan() {
    try (MockedStatic<NamedExpression> utilities = mockStatic(NamedExpression.class)) {
      mockNewExprId(exprIdAccumulator, utilities);
      ExprId datasetDependencyExpression = ExprId.apply(0);
      EqualTo equalTo = new EqualTo((Expression) expression1, (Expression) expression2);
      AttributeReference expression3 = field(NAME3, exprId3);
      GreaterThan greaterThan = new GreaterThan(expression3, new Literal(5, IntegerType$.MODULE$));
      Filter filter = new Filter(new And(equalTo, greaterThan), mock(LogicalPlan.class));

      LogicalPlan plan = new CreateTableAsSelect(null, null, null, filter, null, null, false);
      ExpressionDependencyCollector.collect(context, plan);

      verify(builder, times(1)).addDatasetDependency(datasetDependencyExpression);
      verify(builder, times(1))
          .addDependency(
              datasetDependencyExpression,
              exprId1,
              TransformationInfo.indirect(TransformationInfo.Subtypes.FILTER));
      verify(builder, times(1))
          .addDependency(
              datasetDependencyExpression,
              exprId2,
              TransformationInfo.indirect(TransformationInfo.Subtypes.FILTER));
      verify(builder, times(1))
          .addDependency(
              datasetDependencyExpression,
              exprId3,
              TransformationInfo.indirect(TransformationInfo.Subtypes.FILTER));
      utilities.verify(NamedExpression::newExprId, times(1));
    }
  }

  @Test
  void testCollectFromJoinPlan() {
    try (MockedStatic<NamedExpression> utilities = mockStatic(NamedExpression.class)) {
      mockNewExprId(exprIdAccumulator, utilities);
      ExprId datasetDependencyExpression = ExprId.apply(0);
      EqualTo equalTo = new EqualTo((Expression) expression1, (Expression) expression2);
      Join join =
          new Join(
              mock(LogicalPlan.class),
              mock(LogicalPlan.class),
              JoinType.apply("inner"),
              ScalaConversionUtils.toScalaOption((Expression) equalTo),
              JoinHint.NONE());

      LogicalPlan plan = new CreateTableAsSelect(null, null, null, join, null, null, false);
      ExpressionDependencyCollector.collect(context, plan);

      verify(builder, times(1)).addDatasetDependency(datasetDependencyExpression);
      verify(builder, times(1))
          .addDependency(
              datasetDependencyExpression,
              exprId1,
              TransformationInfo.indirect(TransformationInfo.Subtypes.JOIN));
      verify(builder, times(1))
          .addDependency(
              datasetDependencyExpression,
              exprId2,
              TransformationInfo.indirect(TransformationInfo.Subtypes.JOIN));
      utilities.verify(NamedExpression::newExprId, times(1));
    }
  }

  @Test
  void testCollectFromSortPlan() {
    try (MockedStatic<NamedExpression> utilities = mockStatic(NamedExpression.class)) {
      mockNewExprId(exprIdAccumulator, utilities);
      ExprId datasetDependencyExpression = ExprId.apply(0);
      Sort sort =
          new Sort(
              ScalaConversionUtils.fromList(
                  Collections.singletonList(
                      new SortOrder(
                          (Expression) expression1,
                          mock(SortDirection.class),
                          mock(NullOrdering.class),
                          ScalaConversionUtils.asScalaSeqEmpty()))),
              true,
              mock(LogicalPlan.class));

      LogicalPlan plan = new CreateTableAsSelect(null, null, null, sort, null, null, false);
      ExpressionDependencyCollector.collect(context, plan);

      verify(builder, times(1)).addDatasetDependency(datasetDependencyExpression);
      verify(builder, times(1))
          .addDependency(
              datasetDependencyExpression,
              exprId1,
              TransformationInfo.indirect(TransformationInfo.Subtypes.SORT));
      utilities.verify(NamedExpression::newExprId, times(1));
    }
  }

  @Test
  void CollectFromComplexPlan() {
    try (MockedStatic<NamedExpression> utilities = mockStatic(NamedExpression.class)) {
      mockNewExprId(exprIdAccumulator, utilities);

      AttributeReference expression3 = field(NAME3, exprId3);
      EqualTo equalTo = new EqualTo((Expression) expression1, (Expression) expression2);
      GreaterThan greaterThan = new GreaterThan(expression3, new Literal(5, IntegerType$.MODULE$));

      Join join =
          new Join(
              mock(LogicalPlan.class),
              mock(LogicalPlan.class),
              JoinType.apply("inner"),
              ScalaConversionUtils.toScalaOption((Expression) equalTo),
              JoinHint.NONE());
      Filter filter = new Filter(new And(equalTo, greaterThan), join);
      Sort sort =
          new Sort(
              ScalaConversionUtils.fromList(
                  Collections.singletonList(
                      new SortOrder(
                          (Expression) expression1,
                          mock(SortDirection.class),
                          mock(NullOrdering.class),
                          ScalaConversionUtils.asScalaSeqEmpty()))),
              true,
              filter);

      LogicalPlan plan = new CreateTableAsSelect(null, null, null, sort, null, null, false);
      ExpressionDependencyCollector.collect(context, plan);

      verify(builder, times(1)).addDatasetDependency(ExprId.apply(0));
      verify(builder, times(1)).addDatasetDependency(ExprId.apply(1));
      verify(builder, times(1)).addDatasetDependency(ExprId.apply(2));

      verify(builder, times(1))
          .addDependency(
              ExprId.apply(0),
              exprId1,
              TransformationInfo.indirect(TransformationInfo.Subtypes.SORT));
      verify(builder, times(1))
          .addDependency(
              ExprId.apply(1),
              exprId1,
              TransformationInfo.indirect(TransformationInfo.Subtypes.FILTER));
      verify(builder, times(1))
          .addDependency(
              ExprId.apply(1),
              exprId2,
              TransformationInfo.indirect(TransformationInfo.Subtypes.FILTER));
      verify(builder, times(1))
          .addDependency(
              ExprId.apply(1),
              exprId3,
              TransformationInfo.indirect(TransformationInfo.Subtypes.FILTER));
      verify(builder, times(1))
          .addDependency(
              ExprId.apply(2),
              exprId1,
              TransformationInfo.indirect(TransformationInfo.Subtypes.JOIN));
      verify(builder, times(1))
          .addDependency(
              ExprId.apply(2),
              exprId2,
              TransformationInfo.indirect(TransformationInfo.Subtypes.JOIN));

      utilities.verify(NamedExpression::newExprId, times(3));
    }
  }

  @Test
  void testCollectIFExpressions() {
    If ifExpr =
        new If(
            new EqualTo((Expression) expression1, (Expression) expression2),
            field(NAME3, exprId3),
            field("name4", exprId4));
    Alias res = alias(exprId5, "res", ifExpr);
    Project project = new Project(getNamedExpressionSeq(res), mock(LogicalPlan.class));
    LogicalPlan plan = new CreateTableAsSelect(null, null, null, project, null, null, false);
    ExpressionDependencyCollector.collect(context, plan);

    verify(builder, times(1))
        .addDependency(
            exprId5, exprId1, TransformationInfo.indirect(TransformationInfo.Subtypes.CONDITIONAL));
    verify(builder, times(1))
        .addDependency(
            exprId5, exprId2, TransformationInfo.indirect(TransformationInfo.Subtypes.CONDITIONAL));
    verify(builder, times(1)).addDependency(exprId5, exprId3, TransformationInfo.identity());
    verify(builder, times(1)).addDependency(exprId5, exprId4, TransformationInfo.identity());
  }

  @Test
  void testCollectMultipleDirectTransformationsForOneInput() {
    AttributeReference expression3 = field(NAME3, exprId3);
    If ifExpr =
        new If(
            new EqualTo((Expression) expression1, (Expression) expression2),
            expression3,
            new Add(expression3, new Literal(1, IntegerType$.MODULE$)));
    Alias res = alias(exprId5, "res", ifExpr);
    Project project = new Project(getNamedExpressionSeq(res), mock(LogicalPlan.class));
    LogicalPlan plan = new CreateTableAsSelect(null, null, null, project, null, null, false);
    ExpressionDependencyCollector.collect(context, plan);

    verify(builder, times(1))
        .addDependency(
            exprId5, exprId1, TransformationInfo.indirect(TransformationInfo.Subtypes.CONDITIONAL));
    verify(builder, times(1))
        .addDependency(
            exprId5, exprId2, TransformationInfo.indirect(TransformationInfo.Subtypes.CONDITIONAL));
    verify(builder, times(1)).addDependency(exprId5, exprId3, TransformationInfo.identity());
    verify(builder, times(1)).addDependency(exprId5, exprId3, TransformationInfo.transformation());
  }

  @Test
  void testCollectCaseWhenExpressions() {
    CaseWhen caseWhen =
        new CaseWhen(
            ScalaConversionUtils.fromList(
                Collections.singletonList(
                    ScalaConversionUtils.toScalaTuple(
                        (Expression) expression1, (Expression) expression2))),
            ScalaConversionUtils.toScalaOption((Expression) field(NAME3, exprId3)));
    Alias res = alias(exprId4, "res", caseWhen);
    Project project = new Project(getNamedExpressionSeq(res), mock(LogicalPlan.class));
    LogicalPlan plan = new CreateTableAsSelect(null, null, null, project, null, null, false);
    ExpressionDependencyCollector.collect(context, plan);

    verify(builder, times(1))
        .addDependency(
            exprId4, exprId1, TransformationInfo.indirect(TransformationInfo.Subtypes.CONDITIONAL));
    verify(builder, times(1)).addDependency(exprId4, exprId2, TransformationInfo.identity());
    verify(builder, times(1)).addDependency(exprId4, exprId3, TransformationInfo.identity());
  }

  @Test
  void testCollectMaskingExpressions() {
    If ifExpr =
        new If(
            new EqualTo((Expression) expression1, (Expression) expression2),
            field(NAME3, exprId3),
            new Sha1(field("name4", exprId4)));

    Alias res = alias(exprId5, "name5", ifExpr);

    Project project = new Project(getNamedExpressionSeq(res), mock(LogicalPlan.class));
    LogicalPlan plan = new CreateTableAsSelect(null, null, null, project, null, null, false);
    ExpressionDependencyCollector.collect(context, plan);

    verify(builder, times(1))
        .addDependency(
            exprId5, exprId1, TransformationInfo.indirect(TransformationInfo.Subtypes.CONDITIONAL));
    verify(builder, times(1))
        .addDependency(
            exprId5, exprId2, TransformationInfo.indirect(TransformationInfo.Subtypes.CONDITIONAL));
    verify(builder, times(1)).addDependency(exprId5, exprId3, TransformationInfo.identity());
    verify(builder, times(1))
        .addDependency(exprId5, exprId4, TransformationInfo.transformation(true));
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
    Alias rootAlias = alias(rootAliasExprId, NAME2, (Expression) binaryExpression);

    Project project =
        new Project(
            ScalaConversionUtils.fromList(Collections.singletonList(rootAlias)),
            mock(LogicalPlan.class));
    LogicalPlan plan = new CreateTableAsSelect(null, null, null, project, null, null, false);

    ExpressionDependencyCollector.collect(context, plan);

    verify(builder, times(1))
        .addDependency(rootAliasExprId, exprId1, TransformationInfo.aggregation());
    verify(builder, times(1))
        .addDependency(rootAliasExprId, exprId2, TransformationInfo.aggregation());
  }

  private static Seq<NamedExpression> getNamedExpressionSeq(NamedExpression... expressions) {
    return ScalaConversionUtils.fromList(Arrays.stream(expressions).collect(Collectors.toList()));
  }

  private static Seq<Expression> getExpressionSeq(Expression... expressions) {
    return ScalaConversionUtils.fromList(Arrays.stream(expressions).collect(Collectors.toList()));
  }

  @NotNull
  private AttributeReference field(String name, ExprId exprId) {
    return new AttributeReference(
        name, IntegerType$.MODULE$, false, Metadata$.MODULE$.empty(), exprId, null);
  }

  private static Alias alias(ExprId aliasExprId, String aliasName, NamedExpression child) {
    return alias(aliasExprId, aliasName, (Expression) child);
  }

  @NotNull
  private static Alias alias(ExprId aliasExprId, String aliasName, Expression child) {
    return new Alias(
        child,
        aliasName,
        aliasExprId,
        ScalaConversionUtils.asScalaSeqEmpty(),
        Option.empty(),
        ScalaConversionUtils.asScalaSeqEmpty());
  }

  private static void mockNewExprId(LongAccumulator id, MockedStatic<NamedExpression> utilities) {
    utilities
        .when(NamedExpression::newExprId)
        .thenAnswer(
            (Answer<ExprId>)
                invocation -> {
                  ExprId exprId = ExprId.apply(id.get());
                  id.accumulate(1);
                  return exprId;
                });
  }
}
