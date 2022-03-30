package io.openlineage.spark3.agent.lifecycle.plan.columnLineage;

import io.openlineage.spark.agent.util.ScalaConversionUtils;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.IntStream;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.expressions.BinaryExpression;
import org.apache.spark.sql.catalyst.expressions.ExprId;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.NamedExpression;
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression;
import org.apache.spark.sql.catalyst.plans.logical.Aggregate;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.Project;
import org.apache.spark.sql.catalyst.plans.logical.Union;

/**
 * Traverses LogicalPlan and collects dependencies between the expressions and operations used
 * within the plan.
 */
class FieldDependenciesCollector {

  private final LogicalPlan plan;

  FieldDependenciesCollector(LogicalPlan plan) {
    this.plan = plan;
  }

  void collect(ColumnLevelLineageBuilder builder) {
    plan.foreach(
        node -> {
          List<NamedExpression> expressions = new LinkedList<>();

          if (node instanceof Project) {
            expressions.addAll(
                ScalaConversionUtils.<NamedExpression>fromSeq(((Project) node).projectList()));
          } else if (node instanceof Aggregate) {
            expressions.addAll(
                ScalaConversionUtils.<NamedExpression>fromSeq(
                    ((Aggregate) node).aggregateExpressions()));
          } else if (node instanceof Union) {
            handleUnion((Union) node, builder);
          }

          expressions.stream()
              .forEach(expr -> traverseExpression((Expression) expr, expr.exprId(), builder));

          return scala.runtime.BoxedUnit.UNIT;
        });
  }

  private void traverseExpression(
      Expression expr, ExprId ancestorId, ColumnLevelLineageBuilder builder) {
    if (expr instanceof NamedExpression && !((NamedExpression) expr).exprId().equals(ancestorId)) {
      builder.addDependency(ancestorId, ((NamedExpression) expr).exprId());
    }

    // discover children expression -> handles UnaryExpressions like Alias
    if (expr.children() != null) {
      ScalaConversionUtils.<Expression>fromSeq(expr.children()).stream()
          .forEach(child -> traverseExpression(child, ancestorId, builder));
    }

    if (expr instanceof AggregateExpression) {
      AggregateExpression aggr = (AggregateExpression) expr;
      builder.addDependency(ancestorId, aggr.resultId());
    } else if (expr instanceof BinaryExpression) {
      traverseExpression(((BinaryExpression) expr).left(), ancestorId, builder);
      traverseExpression(((BinaryExpression) expr).right(), ancestorId, builder);
    }
  }

  private void handleUnion(Union union, ColumnLevelLineageBuilder builder) {
    // implement in Java code equivalent to Scala 'children.map(_.output).transpose.map { attrs =>'
    List<LogicalPlan> children = ScalaConversionUtils.<LogicalPlan>fromSeq(union.children());
    List<ArrayList<Attribute>> childrenAttributes = new LinkedList<>();
    children.stream()
        .map(plan -> ScalaConversionUtils.<Attribute>fromSeq(plan.output()))
        .forEach(list -> childrenAttributes.add(new ArrayList<>(list)));

    // max attributes size
    int maxAttributeSize =
        childrenAttributes.stream().map(list -> list.size()).max(Integer::compare).get();

    IntStream.range(0, maxAttributeSize)
        .forEach(
            position -> {
              ExprId firstExpr = childrenAttributes.get(0).get(position).exprId();
              IntStream.range(1, children.size())
                  .mapToObj(childIndex -> childrenAttributes.get(childIndex).get(position))
                  .forEach(attr -> traverseExpression(attr, firstExpr, builder));
            });
  }
}
