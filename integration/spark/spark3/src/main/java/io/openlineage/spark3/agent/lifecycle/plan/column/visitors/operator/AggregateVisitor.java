/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.column.visitors.operator;

import static io.openlineage.client.utils.TransformationInfo.Subtypes.GROUP_BY;

import io.openlineage.client.utils.TransformationInfo;
import io.openlineage.spark.agent.lifecycle.plan.column.ColumnLevelLineageBuilder;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark3.agent.lifecycle.plan.column.ExpressionTraverser;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.expressions.AttributeReference;
import org.apache.spark.sql.catalyst.expressions.ExprId;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.Literal;
import org.apache.spark.sql.catalyst.expressions.NamedExpression;
import org.apache.spark.sql.catalyst.plans.logical.Aggregate;
import org.apache.spark.sql.catalyst.plans.logical.Expand;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.Union;
import org.apache.spark.sql.types.IntegerType;

/**
 * Extracts expression dependencies from an Aggregate operator in {@link LogicalPlan}. Example
 * query:
 *
 * <pre>{@code
 * SELECT dept, SUM(salary) AS total_salary
 * FROM employees
 * GROUP BY dept;
 * }</pre>
 */
public class AggregateVisitor implements OperatorVisitor {

  /** Name of the group id attribute that the RewriteDistinctAggregates rule adds to Expand. */
  private static final String DISTINCT_GROUP_ID = "gid";

  @Override
  public boolean isDefinedAt(LogicalPlan operator) {
    boolean defined = false;
    if (operator instanceof Aggregate) {
      Aggregate aggregate = (Aggregate) operator;
      // don't add group by transformations if child is UNION and aggregate contains group by
      // expressions for all aggregate expressions
      if (!(aggregate.child() instanceof Union && doesGroupByAllAggregateExpressions(aggregate))) {
        defined = true;
      }
    }
    return defined;
  }

  @Override
  public void apply(LogicalPlan operator, ColumnLevelLineageBuilder builder) {
    Aggregate aggregate = (Aggregate) operator;
    if (!isDistinctAggregatesDeduplication(aggregate)) {
      List<Expression> grouppingExpressions =
          ScalaConversionUtils.<Expression>fromSeq((aggregate).groupingExpressions());
      String sql =
          grouppingExpressions.stream().map(Expression::sql).collect(Collectors.joining(", "));
      String outputExpressionString = String.format("GROUP BY %s", sql);

      ExprId exprId = NamedExpression.newExprId();
      builder.addDatasetDependency(exprId, outputExpressionString);

      grouppingExpressions.forEach(
          e ->
              ExpressionTraverser.of(
                      e, exprId, e.sql(), TransformationInfo.indirect(GROUP_BY, e.sql()), builder)
                  .traverse());
    }

    ScalaConversionUtils.<NamedExpression>fromSeq((aggregate).aggregateExpressions())
        .forEach(
            e ->
                ExpressionTraverser.of(
                        (Expression) e,
                        e.exprId(),
                        e.qualifiedName(),
                        TransformationInfo.identity(((Expression) e).sql()),
                        builder)
                    .traverse());
  }

  /**
   * Spark rewrites an aggregation with several DISTINCT columns into two Aggregate nodes over an
   * Expand node. The inner one groups by the grouping keys of the query, the DISTINCT columns and
   * the integer group id that Expand generates, only to deduplicate the rows of each group. Its
   * grouping is not a GROUP BY of the query: the keys of the query are grouped by the outer
   * Aggregate. ROLLUP, CUBE and GROUPING SETS use a single Aggregate over Expand, which groups by
   * spark_grouping_id instead.
   */
  private static boolean isDistinctAggregatesDeduplication(Aggregate aggregate) {
    if (!(aggregate.child() instanceof Expand)) {
      return false;
    }
    List<Expression> groupingExpressions =
        ScalaConversionUtils.<Expression>fromSeq(aggregate.groupingExpressions());
    if (groupingExpressions.isEmpty()) {
      return false;
    }
    Expression last = groupingExpressions.get(groupingExpressions.size() - 1);
    // the key column of an UNPIVOT can be named gid as well, but it is a string
    if (!(last instanceof AttributeReference
        && DISTINCT_GROUP_ID.equals(((AttributeReference) last).name())
        && last.dataType() instanceof IntegerType)) {
      return false;
    }

    // the group id is the attribute that every projection of Expand fills with a literal
    Expand expand = (Expand) aggregate.child();
    List<Attribute> output = ScalaConversionUtils.fromSeq(expand.output());
    int position = -1;
    for (int i = 0; i < output.size(); i++) {
      if (output.get(i).exprId().equals(((AttributeReference) last).exprId())) {
        position = i;
      }
    }
    if (position < 0) {
      return false;
    }
    final int groupIdPosition = position;
    return ScalaConversionUtils.fromSeq(expand.projections()).stream()
        .map(ScalaConversionUtils::fromSeq)
        .allMatch(
            projection ->
                groupIdPosition < projection.size()
                    && projection.get(groupIdPosition) instanceof Literal);
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
