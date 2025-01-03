/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.column.expression;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.calcite.rel.RelNode;

/**
 * Expression to store all the transformations and table fields.
 */
public class ExpressionContainer {

  List<Expression> expressions;

  public ExpressionContainer() {
    this.expressions = new ArrayList<>();
  }

  public void addExpressions(List<Expression> expressions) {
    expressions.addAll(expressions);
  }

  public List<Expression> getOutputsOf(RelNode node) {
    // TODO: can be optimized by using a map in future
    return expressions
        .stream()
        .filter(e -> e.getInputRelNode().equals(node))
        .filter(e -> e instanceof TransformationExpression)
        .map(e -> (TransformationExpression) e)
        .filter(e -> e.getInputRelNode().equals(node))
        .collect(Collectors.toList());
  }

  public void getColumnLineage() {
    // find all table fields
    List<Expression> inputs = expressions
        .stream()
        .filter(e -> e instanceof TableFieldExpression)
        .collect(Collectors.toList());

    // uuids used as inputs of some transformation expressions
    List<UUID> inputUuids = expressions
        .stream()
        .filter(e -> e instanceof TransformationExpression)
        .map(TransformationExpression.class::cast)
        .map(Expression::getUuid).collect(Collectors.toList());


    List<Expression> outputTransformations = expressions
        .stream()
        .filter(e -> getOutputsOf(e.getInputRelNode()).isEmpty())
        .collect(Collectors.toList());

    // TODO: how to attach output transformations with output fields ?
  }
}
