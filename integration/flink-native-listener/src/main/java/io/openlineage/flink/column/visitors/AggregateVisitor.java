/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.column.visitors;

import io.openlineage.flink.column.expression.Expression;
import io.openlineage.flink.column.expression.ExpressionContainer;
import io.openlineage.flink.column.expression.TransformationExpression;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.flink.calcite.shaded.com.google.common.collect.ImmutableList;

public class AggregateVisitor implements CalciteNodeVisitor {

  @Override
  public boolean isDefinedAt(RelNode node) {
    return node instanceof Aggregate;
  }

  @Override
  public List<Expression> loadExpressions(RelNode node, ExpressionContainer expressionContainer) {
    Aggregate aggregate = (Aggregate) node;

    Map<Integer, UUID> inputs =
        expressionContainer.getOutputsOf(aggregate.getInput()).stream()
            .collect(Collectors.toMap(Expression::getOutputRelNodeOrdinal, Expression::getUuid));

    List<RelDataTypeField> fieldList = aggregate.getRowType().getFieldList();
    List<Expression> outputExpressions;

    // items from the group list are the first outputs
    outputExpressions =
        aggregate.getGroupSet().asList().stream()
            .map(
                position ->
                    TransformationExpression.builder()
                        .relNode(aggregate)
                        .uuid(UUID.randomUUID())
                        .inputIds(ImmutableList.of(inputs.get(position)))
                        .outputRelNodeOrdinal(position)
                        .outputName(fieldList.get(position).getName())
                        .build())
            .collect(Collectors.toList());

    // then add the aggregate expressions
    outputExpressions.addAll(
        aggregate.getAggCallList().stream()
            .map(
                aggCall ->
                    TransformationExpression.builder()
                        .relNode(aggregate)
                        .uuid(UUID.randomUUID())
                        .inputIds(
                            aggCall.getArgList().stream()
                                .map(inputs::get)
                                .collect(Collectors.toList()))
                        .outputRelNodeOrdinal(
                            aggregate.getAggCallList().indexOf(aggCall)
                                + aggregate.getGroupSet().cardinality())
                        .transformation(aggCall.getAggregation().toString())
                        .build())
            .collect(Collectors.toList()));

    return outputExpressions;
  }
}
