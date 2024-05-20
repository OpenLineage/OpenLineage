/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.column;

import io.openlineage.spark.agent.lifecycle.plan.column.ColumnLevelLineageContext;
import io.openlineage.spark.agent.util.ExtensionPlanUtils;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.extension.scala.v1.ColumnLevelLineageNode;
import io.openlineage.spark.extension.scala.v1.DatasetFieldLineage;
import io.openlineage.spark.extension.scala.v1.OutputDatasetField;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.expressions.ExprId;
import org.apache.spark.sql.catalyst.expressions.NamedExpression;
import org.apache.spark.sql.catalyst.plans.logical.Aggregate;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.Project;

/** Class created to collect output fields with the corresponding ExprId from LogicalPlan. */
public class OutputFieldsCollector {

  public static void collect(ColumnLevelLineageContext context, LogicalPlan plan) {
    if (plan instanceof ColumnLevelLineageNode) {
      extensionColumnLineage(context, (ColumnLevelLineageNode) plan);
    } else if (plan instanceof io.openlineage.spark.extension.v1.ColumnLevelLineageNode) {
      javaExtensionColumnLineage(
          context, (io.openlineage.spark.extension.v1.ColumnLevelLineageNode) plan);
    } else {
      getOutputExpressionsFromRoot(plan).stream()
          .forEach(expr -> context.getBuilder().addOutput(expr.exprId(), expr.name()));
    }

    CustomCollectorsUtils.collectOutputs(context, plan);

    if (!context.getBuilder().hasOutputs()) {
      // extract outputs from the children
      ScalaConversionUtils.<LogicalPlan>fromSeq(plan.children()).stream()
          .forEach(childPlan -> collect(context, childPlan));
    }
  }

  private static void extensionColumnLineage(
      ColumnLevelLineageContext context, ColumnLevelLineageNode node) {
    ScalaConversionUtils.<DatasetFieldLineage>fromSeq(
            node.columnLevelLineageOutputs(
                    ExtensionPlanUtils.context(context.getEvent(), context.getOlContext()))
                .toSeq())
        .stream()
        .filter(df -> df instanceof OutputDatasetField)
        .forEach(
            o -> {
              OutputDatasetField of = (OutputDatasetField) o;
              context.getBuilder().addOutput(ExprId.apply(of.exprId().exprId()), of.field());
            });
  }

  private static void javaExtensionColumnLineage(
      ColumnLevelLineageContext context,
      io.openlineage.spark.extension.v1.ColumnLevelLineageNode node) {
    node
        .getColumnLevelLineageOutputs(
            ExtensionPlanUtils.javaContext(context.getEvent(), context.getOlContext()))
        .stream()
        .filter(df -> df instanceof io.openlineage.spark.extension.v1.OutputDatasetField)
        .forEach(
            o -> {
              io.openlineage.spark.extension.v1.OutputDatasetField of =
                  (io.openlineage.spark.extension.v1.OutputDatasetField) o;
              context
                  .getBuilder()
                  .addOutput(ExprId.apply(of.getExprId().getExprId()), of.getField());
            });
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
