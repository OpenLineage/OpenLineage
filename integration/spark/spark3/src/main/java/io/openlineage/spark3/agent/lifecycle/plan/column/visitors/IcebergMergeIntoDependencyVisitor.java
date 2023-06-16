/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.column.visitors;

import static io.openlineage.spark3.agent.lifecycle.plan.column.ExpressionDependencyCollector.traverseExpression;

import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark3.agent.lifecycle.plan.column.ColumnLevelLineageBuilder;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
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
  // https://github.com/apache/iceberg/blob/apache-iceberg-1.3.0/spark/v3.4/spark-extensions/src/main/scala/org/apache/spark/sql/catalyst/plans/logical/MergeRows.scala
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

        boolean isNewerImplementation =
            Optional.of((Seq<Seq<Object>>) mergeRows.getMethod("matchedOutputs").invoke(node))
                .filter(seq -> seq.size() > 0)
                .map(seq -> seq.apply(0))
                .filter(seq -> seq.size() > 0)
                .map(seq -> seq.apply(0))
                .filter(el -> el instanceof Seq)
                .isPresent();

        // Older versions of matchedOutputs return Seq[Seq[Expression]], while the newer do
        // return
        // Seq<Seq<Seq<Expression>>>. Older versions of notMatchedOutputs return <Seq<Expression>
        // while the newer do return Seq<Seq<Expression>>.
        if (isNewerImplementation) {
          // newer version
          Seq<Seq<Seq<Expression>>> matched =
              (Seq<Seq<Seq<Expression>>>) mergeRows.getMethod("matchedOutputs").invoke(node);
          Seq<Seq<Expression>> notMatched =
              (Seq<Seq<Expression>>) mergeRows.getMethod("notMatchedOutputs").invoke(node);

          collect(
              node.output(),
              fromSeqNestedThreeTimes(matched).get(0),
              fromSeqNestedTwice(notMatched),
              builder);
          if (matched.size() > 1) {
            collect(
                node.output(),
                fromSeqNestedThreeTimes(matched).get(1),
                fromSeqNestedTwice(notMatched),
                builder);
          }
        } else {
          // older version
          Seq<Seq<Expression>> matched =
              (Seq<Seq<Expression>>) mergeRows.getMethod("matchedOutputs").invoke(node);
          Seq<Seq<Expression>> notMatched =
              (Seq<Seq<Expression>>) mergeRows.getMethod("notMatchedOutputs").invoke(node);
          collect(
              node.output(), fromSeqNestedTwice(matched), fromSeqNestedTwice(notMatched), builder);
        }
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
            ScalaConversionUtils.<Option<Seq<Expression>>>fromSeq(matched).stream()
                .filter(Option::isDefined)
                .map(Option::get)
                .map(is -> ScalaConversionUtils.<Expression>fromSeq(is))
                .collect(Collectors.toList()),
            ScalaConversionUtils.<Option<Seq<Expression>>>fromSeq(notMatched).stream()
                .filter(Option::isDefined)
                .map(Option::get)
                .map(is -> ScalaConversionUtils.<Expression>fromSeq(is))
                .collect(Collectors.toList()),
            builder);
      }

    } catch (Exception e) {
      // do nothing
      log.error("Collecting dependencies for Iceberg MergeInto failed", e);
    }
  }

  void collect(
      Seq<Attribute> outputSeq,
      List<List<Expression>> matched,
      List<List<Expression>> notMatched,
      ColumnLevelLineageBuilder builder) {
    Attribute[] output =
        ScalaConversionUtils.<Attribute>fromSeq(outputSeq).toArray(new Attribute[0]);

    IntStream.range(0, output.length)
        .forEach(
            position -> {
              matched.stream()
                  .filter(exprs -> exprs.size() > position)
                  .map(exprs -> exprs.get(position))
                  .filter(expr -> expr instanceof NamedExpression)
                  .forEach(expr -> traverseExpression(expr, output[position].exprId(), builder));

              notMatched.stream()
                  .filter(exprs -> exprs.size() > position)
                  .map(exprs -> exprs.get(position))
                  .filter(expr -> expr instanceof NamedExpression)
                  .forEach(expr -> traverseExpression(expr, output[position].exprId(), builder));
            });
  }

  private static List<List<List<Expression>>> fromSeqNestedThreeTimes(
      Seq<Seq<Seq<Expression>>> expressions) {
    return ScalaConversionUtils.<Seq<Seq<Expression>>>fromSeq(expressions).stream()
        .map(s -> ScalaConversionUtils.<Seq<Expression>>fromSeq(s))
        .map(
            l ->
                l.stream()
                    .map(is -> ScalaConversionUtils.<Expression>fromSeq(is))
                    .collect(Collectors.toList()))
        .collect(Collectors.toList());
  }

  private static List<List<Expression>> fromSeqNestedTwice(Seq<Seq<Expression>> expressions) {
    return ScalaConversionUtils.<Seq<Expression>>fromSeq(expressions).stream()
        .map(s -> ScalaConversionUtils.<Expression>fromSeq(s))
        .collect(Collectors.toList());
  }
}
