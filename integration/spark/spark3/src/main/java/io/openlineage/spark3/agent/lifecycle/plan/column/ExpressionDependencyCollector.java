/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.column;

import io.openlineage.spark.agent.lifecycle.plan.column.ColumnLevelLineageBuilder;
import io.openlineage.spark.agent.util.ExtensionPlanUtils;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.extension.scala.v1.ColumnLevelLineageNode;
import io.openlineage.spark.extension.scala.v1.ExpressionDependency;
import io.openlineage.spark.extension.scala.v1.ExpressionDependencyWithDelegate;
import io.openlineage.spark.extension.scala.v1.ExpressionDependencyWithIdentifier;
import io.openlineage.spark.extension.scala.v1.OlExprId;
import io.openlineage.spark3.agent.lifecycle.plan.column.visitors.ExpressionDependencyVisitor;
import io.openlineage.spark3.agent.lifecycle.plan.column.visitors.IcebergMergeIntoDependencyVisitor;
import io.openlineage.spark3.agent.lifecycle.plan.column.visitors.UnionDependencyVisitor;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.reflect.MethodUtils;
import org.apache.spark.sql.catalyst.expressions.ExprId;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.NamedExpression;
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression;
import org.apache.spark.sql.catalyst.plans.logical.Aggregate;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.Project;
import org.apache.spark.sql.execution.datasources.LogicalRelation;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCRelation;
import scala.collection.Seq;

/**
 * Traverses LogicalPlan and collects dependencies between the expressions and operations used
 * within the plan.
 */
@Slf4j
public class ExpressionDependencyCollector {

  private static final List<ExpressionDependencyVisitor> expressionDependencyVisitors =
      Arrays.asList(new UnionDependencyVisitor(), new IcebergMergeIntoDependencyVisitor());

  static void collect(
      OpenLineageContext context, LogicalPlan plan, ColumnLevelLineageBuilder builder) {

    plan.foreach(
        node -> {
          collectFromNode(context, node, builder);
          return scala.runtime.BoxedUnit.UNIT;
        });
  }

  static void collectFromNode(
      OpenLineageContext context, LogicalPlan node, ColumnLevelLineageBuilder builder) {
    expressionDependencyVisitors.stream()
        .filter(collector -> collector.isDefinedAt(node))
        .forEach(collector -> collector.apply(node, builder));

    CustomCollectorsUtils.collectExpressionDependencies(context, node, builder);
    List<NamedExpression> expressions = new LinkedList<>();
    if (node instanceof ColumnLevelLineageNode) {
      extensionColumnLineage(context, builder, (ColumnLevelLineageNode) node);
    } else if (node instanceof Project) {
      expressions.addAll(
          ScalaConversionUtils.<NamedExpression>fromSeq(((Project) node).projectList()));
    } else if (node instanceof Aggregate) {
      expressions.addAll(
          ScalaConversionUtils.<NamedExpression>fromSeq(((Aggregate) node).aggregateExpressions()));
    } else if (node instanceof LogicalRelation) {
      if (((LogicalRelation) node).relation() instanceof JDBCRelation) {
        JdbcColumnLineageCollector.extractExpressionsFromJDBC(node, builder);
      }
    }

    expressions.stream()
        .forEach(expr -> traverseExpression((Expression) expr, expr.exprId(), builder));
  }

  private static void extensionColumnLineage(
      OpenLineageContext context, ColumnLevelLineageBuilder builder, ColumnLevelLineageNode node) {
    List<ExpressionDependency> deps =
        ScalaConversionUtils.<ExpressionDependency>fromSeq(
            node.columnLevelLineageDependencies(ExtensionPlanUtils.context(context)).toSeq());

    deps.stream()
        .filter(e -> e instanceof ExpressionDependencyWithDelegate)
        .map(e -> (ExpressionDependencyWithDelegate) e)
        .filter(e -> e.expression() instanceof Expression)
        .forEach(
            e ->
                traverseExpression(
                    (Expression) e.expression(), ExprId.apply(e.outputExprId().exprId()), builder));

    deps.stream()
        .filter(e -> e instanceof ExpressionDependencyWithIdentifier)
        .map(e -> (ExpressionDependencyWithIdentifier) e)
        .forEach(
            d ->
                ScalaConversionUtils.<OlExprId>fromSeq(d.inputExprIds().toSeq()).stream()
                    .forEach(
                        i ->
                            builder.addDependency(
                                ExprId.apply(d.outputExprId().exprId()),
                                ExprId.apply(i.exprId()))));
  }

  public static void traverseExpression(
      Expression expr, ExprId outputExprId, ColumnLevelLineageBuilder builder) {
    if (expr instanceof NamedExpression
        && !((NamedExpression) expr).exprId().equals(outputExprId)) {
      builder.addDependency(outputExprId, ((NamedExpression) expr).exprId());
    }

    // discover children expression -> handles UnaryExpressions like Alias
    if (expr.children() != null) {
      ScalaConversionUtils.<Expression>fromSeq(expr.children()).stream()
          .forEach(child -> traverseExpression(child, outputExprId, builder));
    }

    if (expr instanceof AggregateExpression) {
      AggregateExpression aggr = (AggregateExpression) expr;

      // in databricks `resultId` method is not present. Instead, there exists `resultIds`
      if (MethodUtils.getAccessibleMethod(AggregateExpression.class, "resultId") != null) {
        builder.addDependency(outputExprId, aggr.resultId());
      } else {
        try {
          Seq<ExprId> resultIds = (Seq<ExprId>) MethodUtils.invokeMethod(aggr, "resultIds");
          ScalaConversionUtils.<ExprId>fromSeq(resultIds).stream()
              .forEach(e -> builder.addDependency(outputExprId, e));
        } catch (Exception e) {
          // do nothing
          log.warn("Failed extracting resultIds from AggregateExpression", e);
        }
      }
    }
  }
}
