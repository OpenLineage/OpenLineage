/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.column;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import io.openlineage.spark.agent.lifecycle.plan.column.ColumnLevelLineageBuilder;
import io.openlineage.spark.agent.lifecycle.plan.column.ColumnLevelLineageContext;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.extension.scala.v1.ColumnLevelLineageNode;
import io.openlineage.spark.extension.scala.v1.ExpressionDependency;
import io.openlineage.spark.extension.scala.v1.ExpressionDependencyWithDelegate;
import io.openlineage.spark.extension.scala.v1.ExpressionDependencyWithIdentifier;
import io.openlineage.spark.extension.scala.v1.OlExprId;
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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import scala.Option;
import scala.collection.immutable.Seq;

class ExpressionDependencyCollectorTest {

  ColumnLevelLineageBuilder builder = Mockito.mock(ColumnLevelLineageBuilder.class);
  ColumnLevelLineageContext context = mock(ColumnLevelLineageContext.class);

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

  @BeforeEach
  void setup() {
    when(context.getBuilder()).thenReturn(builder);
    when(context.getOlContext()).thenReturn(mock(OpenLineageContext.class));
  }

  @Test
  void testCollectFromProjectPlan() {
    Project project =
        new Project(
            ScalaConversionUtils.fromList(
                Arrays.asList((NamedExpression) alias1, (NamedExpression) alias2)),
            mock(LogicalPlan.class));
    LogicalPlan plan = new CreateTableAsSelect(null, null, null, project, null, null, false);

    ExpressionDependencyCollector.collect(context, plan);

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

    ExpressionDependencyCollector.collect(context, plan);

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

    ExpressionDependencyCollector.collect(context, plan);

    verify(builder, times(1)).addDependency(rootAliasExprId, exprId1);
    verify(builder, times(1)).addDependency(rootAliasExprId, exprId2);
  }

  @Test
  void testExtensionColumnLevelLineageWithIdentifier() {
    LogicalPlan columnLineagePlanNode =
        mock(LogicalPlan.class, withSettings().extraInterfaces(ColumnLevelLineageNode.class));

    OlExprId outputExprId = new OlExprId(1L);
    OlExprId inputExprId1 = new OlExprId(2L);
    OlExprId inputExprId2 = new OlExprId(3L);

    when(((ColumnLevelLineageNode) columnLineagePlanNode).columnLevelLineageDependencies(any()))
        .thenReturn(
            ScalaConversionUtils.fromList(
                    Collections.<ExpressionDependency>singletonList(
                        new ExpressionDependencyWithIdentifier(
                            outputExprId,
                            ScalaConversionUtils.fromList(Arrays.asList(inputExprId1, inputExprId2))
                                .toList())))
                .toList());

    ExpressionDependencyCollector.collectFromNode(context, columnLineagePlanNode);

    verify(builder, times(1)).addDependency(ExprId.apply(1L), ExprId.apply(2L));
    verify(builder, times(1)).addDependency(ExprId.apply(1L), ExprId.apply(3L));
  }

  @Test
  void testExtensionColumnLevelLineageWithDelegate() {
    LogicalPlan columnLineagePlanNode =
        mock(LogicalPlan.class, withSettings().extraInterfaces(ColumnLevelLineageNode.class));

    when(((ColumnLevelLineageNode) columnLineagePlanNode).columnLevelLineageDependencies(any()))
        .thenReturn(
            ScalaConversionUtils.fromList(
                    Collections.<ExpressionDependency>singletonList(
                        new ExpressionDependencyWithDelegate(
                            new OlExprId(1L),
                            new AttributeReference(
                                "name1",
                                IntegerType$.MODULE$,
                                false,
                                Metadata$.MODULE$.empty(),
                                ExprId.apply(2L),
                                null))))
                .toList());

    ExpressionDependencyCollector.collectFromNode(context, columnLineagePlanNode);

    verify(builder, times(1)).addDependency(ExprId.apply(1L), ExprId.apply(2L));
  }
}
