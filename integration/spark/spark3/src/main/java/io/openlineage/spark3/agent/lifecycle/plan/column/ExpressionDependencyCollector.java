/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.column;

import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark3.agent.lifecycle.plan.column.visitors.ExpressionDependencyVisitor;
import io.openlineage.spark3.agent.lifecycle.plan.column.visitors.IcebergMergeIntoDependencyVisitor;
import io.openlineage.spark3.agent.lifecycle.plan.column.visitors.UnionDependencyVisitor;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.catalyst.expressions.ExprId;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.NamedExpression;
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression;
import org.apache.spark.sql.catalyst.plans.logical.Aggregate;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.Project;
import org.apache.spark.sql.execution.datasources.LogicalRelation;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCRelation;

/**
 * Traverses LogicalPlan and collects dependencies between the expressions and operations used
 * within the plan.
 */
@Slf4j
public class ExpressionDependencyCollector {

  private static final List<ExpressionDependencyVisitor> expressionDependencyVisitors =
      Arrays.asList(new UnionDependencyVisitor(), new IcebergMergeIntoDependencyVisitor());

  static void collect(LogicalPlan plan, ColumnLevelLineageBuilder builder) {

    plan.foreach(
        node -> {
          expressionDependencyVisitors.stream()
              .filter(collector -> collector.isDefinedAt(node))
              .forEach(collector -> collector.apply(node, builder));

          CustomCollectorsUtils.collectExpressionDependencies(node, builder);
          List<NamedExpression> expressions = new LinkedList<>();
          if (node instanceof Project) {
            expressions.addAll(
                ScalaConversionUtils.<NamedExpression>fromSeq(((Project) node).projectList()));
          } else if (node instanceof Aggregate) {
            expressions.addAll(
                ScalaConversionUtils.<NamedExpression>fromSeq(
                    ((Aggregate) node).aggregateExpressions()));
          } else if (node instanceof LogicalRelation) {
            if (((LogicalRelation) node).relation() instanceof JDBCRelation) {
              JdbcColumnLineageCollector.extractExpressionsFromJDBC(node, builder);
            }
          }

          expressions.stream()
              .forEach(expr -> traverseExpression((Expression) expr, expr.exprId(), builder));
          return scala.runtime.BoxedUnit.UNIT;
        });
  }

  public static void traverseExpression(
      Expression expr, ExprId ancestorId, ColumnLevelLineageBuilder builder) {
    if (expr instanceof NamedExpression && !((NamedExpression) expr).exprId().equals(ancestorId)) {
      builder.addDependency(ancestorId, ((NamedExpression) expr).exprId());
    }

    // discover children expression -> handles UnaryExpressions like Alias
    if (expr.children() != null) {
      ScalaConversionUtils.<Expression>fromSeq(expr.children()).stream()
          .forEach(child -> traverseExpression(child, ancestorId, builder));
    }

    if (expr instanceof AggregateExpression) {
      AggregateExpression aggr = (AggregateExpression) expr;
      builder.addDependency(ancestorId, aggr.resultId());
    }
  }
}
