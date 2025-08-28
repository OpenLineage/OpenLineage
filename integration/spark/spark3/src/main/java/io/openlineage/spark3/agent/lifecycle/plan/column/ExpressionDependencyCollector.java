/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.column;

import static io.openlineage.client.utils.TransformationInfo.Subtypes.CONDITIONAL;
import static io.openlineage.client.utils.TransformationInfo.Subtypes.WINDOW;

import io.openlineage.client.utils.TransformationInfo;
import io.openlineage.spark.agent.lifecycle.plan.column.ColumnLevelLineageBuilder;
import io.openlineage.spark.agent.lifecycle.plan.column.ColumnLevelLineageContext;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark3.agent.lifecycle.plan.column.visitors.ExpressionVisitor;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.reflect.MethodUtils;
import org.apache.spark.sql.catalyst.expressions.Alias;
import org.apache.spark.sql.catalyst.expressions.AttributeReference;
import org.apache.spark.sql.catalyst.expressions.CaseWhen;
import org.apache.spark.sql.catalyst.expressions.Coalesce;
import org.apache.spark.sql.catalyst.expressions.Crc32;
import org.apache.spark.sql.catalyst.expressions.ExprId;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.HiveHash;
import org.apache.spark.sql.catalyst.expressions.If;
import org.apache.spark.sql.catalyst.expressions.Md5;
import org.apache.spark.sql.catalyst.expressions.Murmur3Hash;
import org.apache.spark.sql.catalyst.expressions.RankLike;
import org.apache.spark.sql.catalyst.expressions.RowNumberLike;
import org.apache.spark.sql.catalyst.expressions.Sha1;
import org.apache.spark.sql.catalyst.expressions.Sha2;
import org.apache.spark.sql.catalyst.expressions.WindowExpression;
import org.apache.spark.sql.catalyst.expressions.XxHash64;
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression;
import org.apache.spark.sql.catalyst.expressions.aggregate.Count;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import scala.Tuple2;
import scala.collection.Seq;

/**
 * Traverses LogicalPlan and collects dependencies between the expressions and operations used
 * within the plan.
 */
@Slf4j
public class ExpressionDependencyCollector {
  // Generally available masking expressions
  private static final List<Class> classes =
      Arrays.asList(
          Crc32.class,
          HiveHash.class,
          Md5.class,
          Murmur3Hash.class,
          Sha1.class,
          Sha2.class,
          XxHash64.class,
          Count.class);

  // Masking expressions not available in all supported versions
  private static final List<String> classNames =
      Collections.singletonList("org.apache.spark.sql.catalyst.expressions.Mask");

  private static Boolean isMasking(Expression expression) {
    return classes.stream().anyMatch(c -> c.equals(expression.getClass()))
        || classNames.stream().anyMatch(n -> n.equals(expression.getClass().getCanonicalName()));
  }

  private static final VisitorFactory visitorFactory = new VisitorFactory();

  static void collect(ColumnLevelLineageContext context, LogicalPlan plan) {
    plan.foreach(
        node -> {
          CustomCollectorsUtils.collectExpressionDependencies(context, node);
          collectFromNode(context.getBuilder(), node);
          return scala.runtime.BoxedUnit.UNIT;
        });
  }

  public static void collectFromNode(ColumnLevelLineageBuilder builder, LogicalPlan node) {
    visitorFactory.nodeVisitors().stream()
        .filter(collector -> collector.isDefinedAt(node))
        .forEach(collector -> collector.apply(node, builder));
  }

  public static void traverseExpression(
      Expression expr, ExprId outputExprId, ColumnLevelLineageBuilder builder) {
    traverseExpression(expr, outputExprId, TransformationInfo.identity(), builder);
  }

  public static void traverseExpression(
      Expression expr,
      ExprId outputExprId,
      TransformationInfo transformationInfo,
      ColumnLevelLineageBuilder builder) {

    List<ExpressionVisitor> visitors = visitorFactory.expressionVisitors();
    for (ExpressionVisitor v : visitors) {
      if (v.isDefinedAt(expr)) {
        v.apply(expr, builder);
        return;
      }
    }

    if (expr instanceof AttributeReference) {
      AttributeReference attRef = (AttributeReference) expr;
      if (!attRef.exprId().equals(outputExprId)) {
        builder.addDependency(outputExprId, attRef.exprId(), transformationInfo);
      }
    } else if (expr instanceof Alias) {
      handleExpression((Alias) expr, outputExprId, transformationInfo, builder);
    } else if (expr instanceof CaseWhen) {
      handleExpression((CaseWhen) expr, outputExprId, transformationInfo, builder);
    } else if (expr instanceof If) {
      handleExpression((If) expr, outputExprId, transformationInfo, builder);
    } else if (expr instanceof Coalesce) {
      handleExpression((Coalesce) expr, outputExprId, transformationInfo, builder);
    } else if (expr instanceof AggregateExpression) {
      handleExpression((AggregateExpression) expr, outputExprId, transformationInfo, builder);
    } else if (expr instanceof WindowExpression) {
      handleExpression((WindowExpression) expr, outputExprId, transformationInfo, builder);
    } else if (expr != null && expr.children() != null) {
      handleGenericExpression(expr, outputExprId, transformationInfo, builder);
    }
  }

  private static void handleGenericExpression(
      Expression expr,
      ExprId outputExprId,
      TransformationInfo transformationInfo,
      ColumnLevelLineageBuilder builder) {
    ScalaConversionUtils.<Expression>fromSeq(expr.children()).stream()
        .forEach(
            child ->
                traverseExpression(
                    child,
                    outputExprId,
                    transformationInfo.merge(TransformationInfo.transformation(isMasking(expr))),
                    builder));
  }

  private static void handleExpression(
      AggregateExpression expr,
      ExprId outputExprId,
      TransformationInfo transformationInfo,
      ColumnLevelLineageBuilder builder) {
    AggregateExpression aggr = expr;

    // in databricks `resultId` method is not present. Instead, there exists `resultIds`
    if (MethodUtils.getAccessibleMethod(AggregateExpression.class, "resultId") != null) {
      builder.addDependency(
          outputExprId,
          aggr.resultId(),
          transformationInfo.merge(TransformationInfo.aggregation()));
    } else {
      try {
        Seq<ExprId> resultIds = (Seq<ExprId>) MethodUtils.invokeMethod(aggr, "resultIds");
        ScalaConversionUtils.<ExprId>fromSeq(resultIds).stream()
            .forEach(
                e ->
                    builder.addDependency(
                        outputExprId,
                        e,
                        transformationInfo.merge(TransformationInfo.aggregation())));
      } catch (Exception e) {
        // do nothing
        log.warn("Failed extracting resultIds from AggregateExpression", e);
      }
    }
    traverseExpression(
        aggr.aggregateFunction(), outputExprId,
        transformationInfo.merge(TransformationInfo.aggregation()), builder);
  }

  private static void handleExpression(
      If expr,
      ExprId outputExprId,
      TransformationInfo transformationInfo,
      ColumnLevelLineageBuilder builder) {
    If i = expr;
    traverseExpression(
        i.predicate(), outputExprId,
        transformationInfo.merge(TransformationInfo.indirect(CONDITIONAL)), builder);
    traverseExpression(i.trueValue(), outputExprId, transformationInfo, builder);
    traverseExpression(i.falseValue(), outputExprId, transformationInfo, builder);
  }

  private static void handleExpression(
      CaseWhen expr,
      ExprId outputExprId,
      TransformationInfo transformationInfo,
      ColumnLevelLineageBuilder builder) {
    CaseWhen cw = expr;
    List<Tuple2<Expression, Expression>> branches =
        ScalaConversionUtils.<Tuple2<Expression, Expression>>fromSeq(cw.branches());
    branches.stream()
        .map(e -> e._1)
        .forEach(
            e ->
                traverseExpression(
                    e,
                    outputExprId,
                    transformationInfo.merge(TransformationInfo.indirect(CONDITIONAL)),
                    builder));
    branches.stream()
        .map(e -> e._2)
        .forEach(e -> traverseExpression(e, outputExprId, transformationInfo, builder));
    if (cw.elseValue().isDefined()) {
      traverseExpression(cw.elseValue().get(), outputExprId, transformationInfo, builder);
    }
  }

  private static void handleExpression(
      Coalesce expr,
      ExprId outputExprId,
      TransformationInfo transformationInfo,
      ColumnLevelLineageBuilder builder) {
    ScalaConversionUtils.fromSeq(expr.children())
        .forEach(
            e -> {
              traverseExpression(
                  e,
                  outputExprId,
                  transformationInfo.merge(TransformationInfo.indirect(CONDITIONAL)),
                  builder);
              traverseExpression(e, outputExprId, transformationInfo, builder);
            });
  }

  private static void handleExpression(
      WindowExpression expr,
      ExprId outputExprId,
      TransformationInfo transformationInfo,
      ColumnLevelLineageBuilder builder) {
    Expression expression = expr.windowFunction();
    // in case of e.g. RANK() OVER (... ORDER BY X) we can get X as child of Rank
    // even though value of rank is only indirectly dependent of X
    // so in case of RankLike and RowNumberLike we omit possible children
    if (!(expression instanceof RankLike || expression instanceof RowNumberLike)) {
      traverseExpression(
          expr.windowFunction(),
          outputExprId,
          transformationInfo.merge(TransformationInfo.transformation()),
          builder);
    }
    ScalaConversionUtils.<Expression>fromSeq(expr.windowSpec().children())
        .forEach(
            child ->
                traverseExpression(
                    child,
                    outputExprId,
                    transformationInfo.merge(TransformationInfo.indirect(WINDOW)),
                    builder));
  }

  private static void handleExpression(
      Alias expr,
      ExprId outputExprId,
      TransformationInfo transformationInfo,
      ColumnLevelLineageBuilder builder) {
    Alias alias = expr;
    traverseExpression(
        alias.child(), outputExprId,
        transformationInfo.merge(TransformationInfo.identity()), builder);
  }
}
