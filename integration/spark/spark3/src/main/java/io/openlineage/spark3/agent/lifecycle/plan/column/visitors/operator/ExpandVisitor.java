/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.column.visitors.operator;

import io.openlineage.spark.agent.lifecycle.plan.column.ColumnLevelLineageBuilder;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark3.agent.lifecycle.plan.column.ExpressionTraverser;
import java.util.List;
import java.util.stream.IntStream;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.plans.logical.Expand;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;

/**
 * Extracts expression dependencies from Expand operator in {@link LogicalPlan}.
 *
 * <p>The Expand operator applies multiple projections to each input row, producing multiple output
 * rows. It appears in the optimized logical plan for:
 *
 * <ul>
 *   <li><b>GROUP BY CUBE(...)</b> operations
 *   <li><b>GROUP BY ROLLUP(...)</b> operations
 *   <li><b>GROUP BY GROUPING SETS(...)</b> operations
 *   <li><b>UNPIVOT</b> operations (resolved to Expand by Analyzer.ResolveUnpivot)
 * </ul>
 *
 * <p>Example optimized plan for GROUPING SETS:
 *
 * <pre>
 * Aggregate [a#15L, b#16L, spark_grouping_id#14L], [a#15L, b#16L, count(1) AS cnt#10L]
 * +- Expand [[a#6L, b#7L, 0], [a#6L, null, 1], [null, b#7L, 2], [null, null, 3]],
 *            [a#15L, b#16L, spark_grouping_id#14L]
 *    +- Relation [a#6L,b#7L]
 * </pre>
 *
 * <p>For column lineage, each output attribute depends on the expressions at the corresponding
 * position across all projection rows. Non-literal expressions (AttributeReferences) create
 * dependencies.
 *
 * @see <a
 *     href="https://github.com/apache/spark/blob/v3.2.1/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/plans/logical/basicLogicalOperators.scala#L1340">Spark
 *     code</a>
 */
@Slf4j
public class ExpandVisitor implements OperatorVisitor {

  @Override
  public boolean isDefinedAt(LogicalPlan operator) {
    return operator instanceof Expand;
  }

  @Override
  public void apply(LogicalPlan operator, ColumnLevelLineageBuilder builder) {
    Expand expand = (Expand) operator;

    // Get all projection rows - each row is a list of expressions
    List<List<Expression>> projections =
        ScalaConversionUtils.<Expression>fromSeqSeq(expand.projections());

    // Get output attributes
    List<Attribute> output = ScalaConversionUtils.<Attribute>fromSeq(expand.output());

    // For each output attribute position, collect expressions from all projection rows
    // at that position and traverse them to extract dependencies
    IntStream.range(0, output.size())
        .forEach(
            position -> {
              Attribute outputAttr = output.get(position);

              // Collect all expressions at this position across all projection rows
              projections.forEach(
                  projection -> {
                    if (position < projection.size()) {
                      Expression expr = projection.get(position);
                      // Traverse the expression to extract attribute dependencies
                      ExpressionTraverser.of(expr, outputAttr.exprId(), builder).traverse();
                    }
                  });
            });
  }
}
