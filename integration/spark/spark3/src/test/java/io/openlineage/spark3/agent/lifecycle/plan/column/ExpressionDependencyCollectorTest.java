/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.column;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import io.openlineage.client.utils.TransformationInfo;
import io.openlineage.spark.agent.lifecycle.plan.column.ColumnLevelLineageBuilder;
import io.openlineage.spark.agent.lifecycle.plan.column.ColumnLevelLineageContext;
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
import org.apache.spark.sql.catalyst.expressions.Coalesce;
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
import org.apache.spark.sql.catalyst.plans.JoinType;
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

  final String ALIAS_NAME = "res";
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
    Alias res = alias(exprId5, ALIAS_NAME, ifExpr);
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
    Alias res = alias(exprId5, ALIAS_NAME, ifExpr);
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
    Alias res = alias(exprId4, ALIAS_NAME, caseWhen);
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

    Alias res = alias(exprId5, ALIAS_NAME, ifExpr);

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

  @Test
  void testCollectCoalesceExpressions() {
    Coalesce coalesceExpr =
        new Coalesce(getExpressionSeq((Expression) expression1, (Expression) expression2));
    Alias res = alias(exprId3, ALIAS_NAME, coalesceExpr);
    Project project = new Project(getNamedExpressionSeq(res), mock(LogicalPlan.class));
    LogicalPlan plan = new CreateTableAsSelect(null, null, null, project, null, null, false);
    ExpressionDependencyCollector.collect(context, plan);

    verify(builder, times(1))
        .addDependency(
            exprId3, exprId1, TransformationInfo.indirect(TransformationInfo.Subtypes.CONDITIONAL));
    verify(builder, times(1))
        .addDependency(
            exprId3, exprId2, TransformationInfo.indirect(TransformationInfo.Subtypes.CONDITIONAL));
    verify(builder, times(1)).addDependency(exprId3, exprId1, TransformationInfo.identity());
    verify(builder, times(1)).addDependency(exprId3, exprId2, TransformationInfo.identity());
    verifyNoMoreInteractions(builder);
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
        name,
        IntegerType$.MODULE$,
        false,
        Metadata$.MODULE$.empty(),
        exprId,
        ScalaConversionUtils.asScalaSeqEmpty());
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
