/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.column;

import io.openlineage.spark.agent.lifecycle.plan.column.ColumnLevelLineageBuilder;
import io.openlineage.spark.agent.lifecycle.plan.column.ColumnLevelLineageContext;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
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

  static void collect(ColumnLevelLineageContext context, LogicalPlan plan) {
    plan.foreach(
        node -> {
          collectFromNode(context, node);
          return scala.runtime.BoxedUnit.UNIT;
        });
  }

  static void collectFromNode(ColumnLevelLineageContext context, LogicalPlan node) {
    expressionDependencyVisitors.stream()
        .filter(collector -> collector.isDefinedAt(node))
        .forEach(collector -> collector.apply(node, context.getBuilder()));

    CustomCollectorsUtils.collectExpressionDependencies(context, node);
    List<NamedExpression> expressions = new LinkedList<>();
    if (node instanceof Project) {
      expressions.addAll(
          ScalaConversionUtils.<NamedExpression>fromSeq(((Project) node).projectList()));
    } else if (node instanceof Aggregate) {
      expressions.addAll(
          ScalaConversionUtils.<NamedExpression>fromSeq(((Aggregate) node).aggregateExpressions()));
    } else if (node instanceof LogicalRelation) {
      if (((LogicalRelation) node).relation() instanceof JDBCRelation) {
        JdbcColumnLineageCollector.extractExpressionsFromJDBC(context, node);
      }
    }

    expressions.stream()
        .forEach(
            expr -> traverseExpression((Expression) expr, expr.exprId(), context.getBuilder()));
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
