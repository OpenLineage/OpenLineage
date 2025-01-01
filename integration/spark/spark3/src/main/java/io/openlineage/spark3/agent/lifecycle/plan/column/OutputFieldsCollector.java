/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.column;

import io.openlineage.spark.agent.lifecycle.plan.column.ColumnLevelLineageContext;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import java.util.List;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.expressions.NamedExpression;
import org.apache.spark.sql.catalyst.plans.logical.Aggregate;
import org.apache.spark.sql.catalyst.plans.logical.CreateTableAsSelect;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.Project;

/** Class created to collect output fields with the corresponding ExprId from LogicalPlan. */
@Slf4j
public class OutputFieldsCollector {

  public static void collect(ColumnLevelLineageContext context, LogicalPlan plan) {
    getOutputExpressionsFromRoot(plan).stream()
        .forEach(expr -> context.getBuilder().addOutput(expr.exprId(), expr.name()));

    CustomCollectorsUtils.collectOutputs(context, plan);

    if (!context.getBuilder().hasOutputs()) {
      // extract outputs from the children
      ScalaConversionUtils.<LogicalPlan>fromSeq(plan.children()).stream()
          .forEach(childPlan -> collect(context, childPlan));
    }
  }

  static List<NamedExpression> getOutputExpressionsFromRoot(LogicalPlan plan) {
    List<NamedExpression> expressions =
        ScalaConversionUtils.fromSeq(plan.output()).stream()
            .filter(attr -> attr instanceof Attribute)
            .map(attr -> (Attribute) attr)
            .collect(Collectors.toList());

    if (plan instanceof Aggregate) {
      expressions.addAll(
          ScalaConversionUtils.<NamedExpression>fromSeq(((Aggregate) plan).aggregateExpressions()));
    } else if (plan instanceof Project) {
      expressions.addAll(
          ScalaConversionUtils.<NamedExpression>fromSeq(((Project) plan).projectList()));
    } else if (expressions.isEmpty() && plan instanceof CreateTableAsSelect) {
      return getOutputExpressionsFromRoot(((CreateTableAsSelect) plan).query());
    }

    return expressions;
  }

  static List<NamedExpression> getOutputExpressionsFromTree(LogicalPlan plan) {
    List<NamedExpression> expressions = getOutputExpressionsFromRoot(plan);
    if (expressions == null || expressions.isEmpty()) {
      // extract outputs from the children
      ScalaConversionUtils.<LogicalPlan>fromSeq(plan.children()).stream()
          .forEach(childPlan -> expressions.addAll(getOutputExpressionsFromTree(childPlan)));
    }
    return expressions;
  }
}
