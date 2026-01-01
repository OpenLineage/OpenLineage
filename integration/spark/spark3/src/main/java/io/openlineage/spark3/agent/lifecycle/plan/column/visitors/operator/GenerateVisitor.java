/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.column.visitors.operator;

import io.openlineage.client.utils.TransformationInfo;
import io.openlineage.spark.agent.lifecycle.plan.column.ColumnLevelLineageBuilder;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark3.agent.lifecycle.plan.column.ExpressionTraverser;
import java.util.List;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.plans.logical.Generate;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;

/**
 * Extracts expression dependencies from Generate operator in {@link LogicalPlan}. Example query
 * 'SELECT explode(split(a, ' ')) AS a FROM t)'.
 */
public class GenerateVisitor implements OperatorVisitor {
  @Override
  public boolean isDefinedAt(LogicalPlan operator) {
    return operator instanceof Generate;
  }

  @Override
  public void apply(LogicalPlan operator, ColumnLevelLineageBuilder builder) {
    Generate node = (Generate) operator;
    List<Attribute> attributes = ScalaConversionUtils.fromSeq(node.generatorOutput());
    List<Expression> children =
        ScalaConversionUtils.fromSeq(((Expression) node.generator()).children());
    attributes.forEach(
        ne ->
            children.forEach(
                e ->
                    ExpressionTraverser.of(
                            e, ne.exprId(), TransformationInfo.transformation(), builder)
                        .traverse()));
  }
}
