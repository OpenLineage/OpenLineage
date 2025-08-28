/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.column.visitors.node;

import static io.openlineage.spark3.agent.lifecycle.plan.column.ExpressionDependencyCollector.traverseExpression;

import io.openlineage.client.utils.TransformationInfo;
import io.openlineage.spark.agent.lifecycle.plan.column.ColumnLevelLineageBuilder;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import java.util.List;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.plans.logical.Generate;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;

/**
 * Extracts expression dependencies from Generate node in {@link LogicalPlan}. Example query 'SELECT
 * explode(split(a, ' ')) AS a FROM t)'.
 */
public class GenerateNodeVisitor implements NodeVisitor {
  @Override
  public boolean isDefinedAt(LogicalPlan plan) {
    return plan instanceof Generate;
  }

  @Override
  public void apply(LogicalPlan plan, ColumnLevelLineageBuilder builder) {
    Generate node = (Generate) plan;
    List<Attribute> attributes = ScalaConversionUtils.fromSeq(node.generatorOutput());
    List<Expression> children =
        ScalaConversionUtils.fromSeq(((Expression) node.generator()).children());
    attributes.forEach(
        ne ->
            children.forEach(
                e ->
                    traverseExpression(
                        e, ne.exprId(), TransformationInfo.transformation(), builder)));
  }
}
