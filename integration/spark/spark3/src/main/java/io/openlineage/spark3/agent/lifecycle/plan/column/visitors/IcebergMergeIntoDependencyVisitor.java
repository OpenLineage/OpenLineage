/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.column.visitors;

import static io.openlineage.spark3.agent.lifecycle.plan.column.ExpressionDependencyCollector.traverseExpression;

import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark3.agent.lifecycle.plan.column.ColumnLevelLineageBuilder;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.NamedExpression;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import scala.Option;
import scala.collection.Seq;

/** Custom visitor for `MERGE INTO TABLE` implementation of Iceberg provider. */
@Slf4j
public class IcebergMergeIntoDependencyVisitor implements ExpressionDependencyVisitor {

  // https://github.com/apache/iceberg/blob/apache-iceberg-0.13.1/spark/v3.2/spark-extensions/src/main/scala/org/apache/spark/sql/catalyst/plans/logical/MergeRows.scala
  private static final String MERGE_ROWS_CLASS_NAME =
      "org.apache.spark.sql.catalyst.plans.logical.MergeRows";

  // https://github.com/apache/iceberg/blob/0.13.x/spark/v3.1/spark-extensions/src/main/scala/org/apache/spark/sql/catalyst/plans/logical/MergeInto.scala
  private static final String MERGE_INTO_CLASS_NAME =
      "org.apache.spark.sql.catalyst.plans.logical.MergeInto";

  @Override
  public boolean isDefinedAt(LogicalPlan plan) {
    return Arrays.asList(MERGE_INTO_CLASS_NAME, MERGE_ROWS_CLASS_NAME)
        .contains(plan.getClass().getCanonicalName());
  }

  @Override
  public void apply(LogicalPlan node, ColumnLevelLineageBuilder builder) {
    try {
      String nodeClass = node.getClass().getCanonicalName();

      if (MERGE_ROWS_CLASS_NAME.equals(nodeClass)) {
        Class mergeRows = Class.forName(MERGE_ROWS_CLASS_NAME);
        Seq<Seq<Expression>> matched =
            (Seq<Seq<Expression>>) mergeRows.getMethod("matchedOutputs").invoke(node);
        Seq<Seq<Expression>> notMatched =
            (Seq<Seq<Expression>>) mergeRows.getMethod("notMatchedOutputs").invoke(node);
        collect(
            node.output(),
            ScalaConversionUtils.<Seq<Expression>>fromSeq(matched).stream()
                .map(e -> Option.apply(e))
                .collect(Collectors.toList()),
            ScalaConversionUtils.<Seq<Expression>>fromSeq(notMatched).stream()
                .map(e -> Option.apply(e))
                .collect(Collectors.toList()),
            builder);
      } else if (MERGE_INTO_CLASS_NAME.equals(nodeClass)) {
        Class mergeInto = Class.forName("org.apache.spark.sql.catalyst.plans.logical.MergeInto");
        Class mergeIntoParamsClass =
            Class.forName("org.apache.spark.sql.catalyst.plans.logical.MergeIntoParams");
        Object mergeIntoParams = mergeInto.getMethod("mergeIntoProcessor").invoke(node);

        Seq<Option<Seq<Expression>>> matched =
            (Seq<Option<Seq<Expression>>>)
                mergeIntoParamsClass.getMethod("matchedOutputs").invoke(mergeIntoParams);
        Seq<Option<Seq<Expression>>> notMatched =
            (Seq<Option<Seq<Expression>>>)
                mergeIntoParamsClass.getMethod("notMatchedOutputs").invoke(mergeIntoParams);

        collect(
            node.output(),
            ScalaConversionUtils.<Option<Seq<Expression>>>fromSeq(matched),
            ScalaConversionUtils.<Option<Seq<Expression>>>fromSeq(notMatched),
            builder);
      }

    } catch (Exception e) {
      // do nothing
      log.error("Collecting dependencies for Iceberg MergeInto failed", e);
    }
  }

  void collect(
      Seq<Attribute> outputSeq,
      List<Option<Seq<Expression>>> matched,
      List<Option<Seq<Expression>>> notMatched,
      ColumnLevelLineageBuilder builder) {
    Attribute[] output =
        ScalaConversionUtils.<Attribute>fromSeq(outputSeq).toArray(new Attribute[0]);

    IntStream.range(0, output.length)
        .forEach(
            position -> {
              matched.stream()
                  .filter(Option::isDefined)
                  .map(opt -> opt.get())
                  .filter(exprs -> exprs.size() > position)
                  .map(exprs -> ScalaConversionUtils.<Expression>fromSeq(exprs).get(position))
                  .filter(expr -> expr instanceof NamedExpression)
                  .forEach(expr -> traverseExpression(expr, output[position].exprId(), builder));

              notMatched.stream()
                  .filter(Option::isDefined)
                  .map(opt -> opt.get())
                  .filter(exprs -> exprs.size() > position)
                  .map(exprs -> ScalaConversionUtils.<Expression>fromSeq(exprs).get(position))
                  .filter(expr -> expr instanceof NamedExpression)
                  .forEach(expr -> traverseExpression(expr, output[position].exprId(), builder));
            });
  }
}
