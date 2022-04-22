package io.openlineage.spark3.agent.lifecycle.plan.columnLineage;

import io.openlineage.spark.agent.util.ScalaConversionUtils;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.expressions.NamedExpression;
import org.apache.spark.sql.catalyst.plans.logical.Aggregate;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.Project;

/** Class created to collect output fields with the corresponding ExprId from LogicalPlan. */
class OutputFieldsCollector {

  private final LogicalPlan plan;

  OutputFieldsCollector(LogicalPlan plan) {
    this.plan = plan;
  }

  void collect(ColumnLevelLineageBuilder builder) {
    collect(plan, builder);
  }

  private void collect(LogicalPlan plan, ColumnLevelLineageBuilder builder) {
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

    expressions.stream().forEach(expr -> builder.addOutput(expr.exprId(), expr.name()));

    if (!builder.hasOutputs()) {
      // extract outputs from the children
      ScalaConversionUtils.<LogicalPlan>fromSeq(plan.children()).stream()
          .forEach(childPlan -> collect(childPlan, builder));
    }
  }
}
