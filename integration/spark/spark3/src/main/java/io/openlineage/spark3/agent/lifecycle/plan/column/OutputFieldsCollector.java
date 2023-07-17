/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.column;

import io.openlineage.spark.agent.lifecycle.plan.column.ColumnLevelLineageBuilder;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.api.OpenLineageContext;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.expressions.NamedExpression;
import org.apache.spark.sql.catalyst.plans.logical.Aggregate;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.Project;

/** Class created to collect output fields with the corresponding ExprId from LogicalPlan. */
public class OutputFieldsCollector {

  public static void collect(
      OpenLineageContext context, LogicalPlan plan, ColumnLevelLineageBuilder builder) {
    getOutputExpressionsFromRoot(plan).stream()
        .forEach(expr -> builder.addOutput(expr.exprId(), expr.name()));
    CustomCollectorsUtils.collectOutputs(context, plan, builder);

    if (!builder.hasOutputs()) {
      // extract outputs from the children
      ScalaConversionUtils.<LogicalPlan>fromSeq(plan.children()).stream()
          .forEach(childPlan -> collect(context, childPlan, builder));
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
