/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.column.visitors.node;

import static io.openlineage.client.utils.TransformationInfo.Subtypes.GROUP_BY;
import static io.openlineage.spark3.agent.lifecycle.plan.column.ExpressionDependencyCollector.traverseExpression;

import io.openlineage.client.utils.TransformationInfo;
import io.openlineage.spark.agent.lifecycle.plan.column.ColumnLevelLineageBuilder;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.spark.sql.catalyst.expressions.AttributeReference;
import org.apache.spark.sql.catalyst.expressions.ExprId;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.NamedExpression;
import org.apache.spark.sql.catalyst.plans.logical.Aggregate;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.Union;

/**
 * Extracts expression dependencies from an Aggregate node in {@link LogicalPlan}. Example query:
 *
 * <pre>{@code
 * SELECT dept, SUM(salary) AS total_salary
 * FROM employees
 * GROUP BY dept;
 * }</pre>
 */
public class AggregateNodeVisitor implements NodeVisitor {
  @Override
  public boolean isDefinedAt(LogicalPlan plan) {
    boolean defined = false;
    if (plan instanceof Aggregate) {
      Aggregate aggregate = (Aggregate) plan;
      // don't add group by transformations if child is UNION and aggregate contains group by
      // expressions for all aggregate expressions
      if (!(aggregate.child() instanceof Union && doesGroupByAllAggregateExpressions(aggregate))) {
        defined = true;
      }
    }
    return defined;
  }

  @Override
  public void apply(LogicalPlan plan, ColumnLevelLineageBuilder builder) {
    Aggregate aggregate = (Aggregate) plan;

    ExprId exprId = NamedExpression.newExprId();
    builder.addDatasetDependency(exprId);

    ScalaConversionUtils.<Expression>fromSeq((aggregate).groupingExpressions())
        .forEach(
            e -> traverseExpression(e, exprId, TransformationInfo.indirect(GROUP_BY), builder));

    ScalaConversionUtils.<NamedExpression>fromSeq((aggregate).aggregateExpressions())
        .forEach(
            e ->
                traverseExpression(
                    (Expression) e, e.exprId(), TransformationInfo.identity(), builder));
  }

  /**
   * Method verifies if an aggregate node has the same aggregate expressions and group by
   * expressions. This can be helpful when determining if an aggregate is used as distinct which
   * should not produce group by column level lineage transformations.
   */
  private static boolean doesGroupByAllAggregateExpressions(Aggregate aggregate) {
    Set<ExprId> aggregateExprIds =
        ScalaConversionUtils.<NamedExpression>fromSeq(aggregate.aggregateExpressions()).stream()
            .map(NamedExpression::exprId)
            .collect(Collectors.toSet());

    Set<ExprId> groupingExprIds =
        ScalaConversionUtils.<Expression>fromSeq(aggregate.groupingExpressions()).stream()
            .filter(e -> e instanceof AttributeReference)
            .map(e -> (AttributeReference) e)
            .map(AttributeReference::exprId)
            .collect(Collectors.toSet());

    return groupingExprIds.containsAll(aggregateExprIds);
  }
}
