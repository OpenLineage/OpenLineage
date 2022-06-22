package io.openlineage.spark3.agent.lifecycle.plan.column.visitors;

import static io.openlineage.spark3.agent.lifecycle.plan.column.ExpressionDependencyCollector.traverseExpression;

import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark3.agent.lifecycle.plan.column.ColumnLevelLineageBuilder;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.IntStream;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.expressions.ExprId;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.Union;

/**
 * Extracts expression dependencies from UNION node in {@link LogicalPlan}. Example query 'SELECT *
 * FROM local.db.t1 UNION SELECT * FROM local.db.t2'. Custom visitor is required because of
 * transpose operation on unioned datasets within Spark's plan:
 *
 * @see <a
 *     href="https://github.com/apache/spark/blob/v3.2.1/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/plans/logical/basicLogicalOperators.scala#L306">Spark
 *     code</a>
 */
@Slf4j
public class UnionDependencyVisitor implements ExpressionDependencyVisitor {

  @Override
  public boolean isDefinedAt(LogicalPlan plan) {
    return plan instanceof Union;
  }

  @Override
  public void apply(LogicalPlan plan, ColumnLevelLineageBuilder builder) {
    Union union = (Union) plan;

    // implement in Java code equivalent to Scala 'children.map(_.output).transpose.map { attrs =>'
    List<LogicalPlan> children = ScalaConversionUtils.<LogicalPlan>fromSeq(union.children());
    List<ArrayList<Attribute>> childrenAttributes = new LinkedList<>();
    children.stream()
        .map(child -> ScalaConversionUtils.<Attribute>fromSeq(child.output()))
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
