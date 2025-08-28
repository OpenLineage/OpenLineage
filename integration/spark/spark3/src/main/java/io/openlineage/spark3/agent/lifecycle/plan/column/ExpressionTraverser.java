/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.column;

import static io.openlineage.client.utils.TransformationInfo.Subtypes.CONDITIONAL;
import static io.openlineage.client.utils.TransformationInfo.Subtypes.WINDOW;
import static io.openlineage.spark.agent.util.ScalaConversionUtils.fromSeq;
import static java.util.Objects.nonNull;

import io.openlineage.client.utils.TransformationInfo;
import io.openlineage.spark.agent.lifecycle.plan.column.ColumnLevelLineageBuilder;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark3.agent.lifecycle.plan.column.visitors.expression.ExpressionVisitor;
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
import scala.Tuple2;
import scala.collection.Seq;

@Slf4j
public class ExpressionTraverser {
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

  private final Expression expression;
  private final ExprId outputExpressionId;
  private final TransformationInfo transformationInfo;
  private final ColumnLevelLineageBuilder builder;
  private final VisitorFactory visitorFactory;

  private ExpressionTraverser(
      Expression expression,
      ExprId outputExpressionId,
      TransformationInfo transformationInfo,
      ColumnLevelLineageBuilder builder,
      VisitorFactory visitorFactory) {
    this.expression = expression;
    this.outputExpressionId = outputExpressionId;
    this.transformationInfo = transformationInfo;
    this.builder = builder;
    this.visitorFactory = visitorFactory;
  }

  public static ExpressionTraverser of(
      Expression expression, ExprId outputExpressionId, ColumnLevelLineageBuilder builder) {
    return ExpressionTraverser.of(
        expression, outputExpressionId, TransformationInfo.identity(), builder);
  }

  public static ExpressionTraverser of(
      Expression expression,
      ExprId outputExpressionId,
      TransformationInfo transformationInfo,
      ColumnLevelLineageBuilder builder) {
    return new ExpressionTraverser(
        expression, outputExpressionId, transformationInfo, builder, new VisitorFactory());
  }

  public ExpressionTraverser copyFor(Expression expression) {
    return ExpressionTraverser.of(
        expression, this.outputExpressionId, this.transformationInfo, this.builder);
  }

  public ExpressionTraverser copyFor(Expression expression, TransformationInfo transformationInfo) {
    return ExpressionTraverser.of(
        expression,
        this.outputExpressionId,
        this.transformationInfo.merge(transformationInfo),
        this.builder);
  }

  public void traverse() {
    List<ExpressionVisitor> visitors = visitorFactory.expressionVisitors();
    if (isLeafNode()) {
      AttributeReference attRef = (AttributeReference) expression;
      if (!attRef.exprId().equals(outputExpressionId)) {
        addDependency(attRef.exprId());
      }
    } else {
      for (ExpressionVisitor v : visitors) {
        if (v.isDefinedAt(expression)) {
          v.apply(expression, this);
          return;
        }
      }
      if (expression instanceof Alias) {
        handleExpression((Alias) expression);
      } else if (expression instanceof CaseWhen) {
        handleExpression((CaseWhen) expression);
      } else if (expression instanceof If) {
        handleExpression((If) expression);
      } else if (expression instanceof Coalesce) {
        handleExpression((Coalesce) expression);
      } else if (expression instanceof AggregateExpression) {
        handleExpression((AggregateExpression) expression);
      } else if (expression instanceof WindowExpression) {
        handleExpression((WindowExpression) expression);
      } else if (shouldFallbackToGenericHandling()) {
        fromSeq(expression.children())
            .forEach(
                child ->
                    copyFor(child, TransformationInfo.transformation(isMasking(expression)))
                        .traverse());
      }
    }
  }

  public void addDependency(ExprId inputExprId) {
    builder.addDependency(outputExpressionId, inputExprId, transformationInfo);
  }

  public void addDependency(ExprId inputExprId, TransformationInfo transformationInfo) {
    builder.addDependency(
        outputExpressionId, inputExprId, this.transformationInfo.merge(transformationInfo));
  }

  private boolean isLeafNode() {
    return expression instanceof AttributeReference;
  }

  private boolean shouldFallbackToGenericHandling() {
    return nonNull(expression) && nonNull(expression.children());
  }

  private void handleExpression(AggregateExpression expr) {
    // in databricks `resultId` method is not present. Instead, there exists `resultIds`
    if (MethodUtils.getAccessibleMethod(AggregateExpression.class, "resultId") != null) {
      addDependency(expr.resultId(), TransformationInfo.aggregation());
    } else {
      try {
        Seq<ExprId> resultIds = (Seq<ExprId>) MethodUtils.invokeMethod(expr, "resultIds");
        ScalaConversionUtils.<ExprId>fromSeq(resultIds)
            .forEach(e -> addDependency(e, TransformationInfo.aggregation()));
      } catch (Exception e) {
        // do nothing
        log.warn("Failed extracting resultIds from AggregateExpression", e);
      }
    }
    copyFor(expr.aggregateFunction(), TransformationInfo.aggregation()).traverse();
  }

  private void handleExpression(If expr) {
    copyFor(expr.predicate(), TransformationInfo.indirect(CONDITIONAL)).traverse();
    copyFor(expr.trueValue()).traverse();
    copyFor(expr.falseValue()).traverse();
  }

  private void handleExpression(CaseWhen expr) {
    List<Tuple2<Expression, Expression>> branches =
        ScalaConversionUtils.<Tuple2<Expression, Expression>>fromSeq(expr.branches());
    branches.stream()
        .map(e -> e._1)
        .forEach(e -> copyFor(e, TransformationInfo.indirect(CONDITIONAL)).traverse());
    branches.stream().map(e -> e._2).forEach(e -> copyFor(e).traverse());
    if (expr.elseValue().isDefined()) {
      copyFor(expr.elseValue().get()).traverse();
    }
  }

  private void handleExpression(Coalesce expr) {
    ScalaConversionUtils.fromSeq(expr.children())
        .forEach(
            e -> {
              copyFor(e, TransformationInfo.indirect(CONDITIONAL)).traverse();
              copyFor(e).traverse();
            });
  }

  private void handleExpression(WindowExpression expr) {
    Expression expression = expr.windowFunction();
    // in case of e.g. RANK() OVER (... ORDER BY X) we can get X as child of Rank
    // even though value of rank is only indirectly dependent of X
    // so in case of RankLike and RowNumberLike we omit possible children
    if (!(expression instanceof RankLike || expression instanceof RowNumberLike)) {
      copyFor(expr.windowFunction(), TransformationInfo.transformation()).traverse();
    }
    ScalaConversionUtils.fromSeq(expr.windowSpec().children())
        .forEach(child -> copyFor(child, TransformationInfo.indirect(WINDOW)).traverse());
  }

  private void handleExpression(Alias expr) {
    copyFor(expr.child(), TransformationInfo.identity()).traverse();
  }
}
