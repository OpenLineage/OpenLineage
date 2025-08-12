/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.column;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.openlineage.spark.agent.lifecycle.plan.column.ColumnLevelLineageBuilder;
import io.openlineage.spark.agent.lifecycle.plan.column.ColumnLevelLineageContext;
import io.openlineage.client.utils.TransformationInfo;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.api.OpenLineageContext;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.atomic.LongAccumulator;
import java.util.stream.Collectors;
import org.apache.spark.sql.catalyst.analysis.UnresolvedIdentifier;
import org.apache.spark.sql.catalyst.expressions.Alias;
import org.apache.spark.sql.catalyst.expressions.AttributeReference;
import org.apache.spark.sql.catalyst.expressions.EqualTo;
import org.apache.spark.sql.catalyst.expressions.ExprId;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.If;
import org.apache.spark.sql.catalyst.expressions.Mask;
import org.apache.spark.sql.catalyst.expressions.NamedExpression;
import org.apache.spark.sql.catalyst.plans.logical.CreateTableAsSelect;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.Project;
import org.apache.spark.sql.types.IntegerType$;
import org.apache.spark.sql.types.Metadata$;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
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
  void testCollectMaskExpressions() {
    If ifExpr =
        new If(
            new EqualTo((Expression) expression1, (Expression) expression2),
            field(NAME3, exprId3),
            new Mask(field("name4", exprId4)));

    Alias res = alias(exprId5, "name5", ifExpr);

    Project project = new Project(getNamedExpressionSeq(res), mock(LogicalPlan.class));
    LogicalPlan plan =
        new CreateTableAsSelect(
            new UnresolvedIdentifier(
                ScalaConversionUtils.fromList(Collections.singletonList("test")), false),
            null,
            project,
            null,
            null,
            false,
            false);
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

  private static Seq<NamedExpression> getNamedExpressionSeq(NamedExpression... expressions) {
    return ScalaConversionUtils.fromList(Arrays.stream(expressions).collect(Collectors.toList()));
  }

  @NotNull
  private AttributeReference field(String name, ExprId exprId) {
    return new AttributeReference(
        name, IntegerType$.MODULE$, false, Metadata$.MODULE$.empty(), exprId, null);
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
}
