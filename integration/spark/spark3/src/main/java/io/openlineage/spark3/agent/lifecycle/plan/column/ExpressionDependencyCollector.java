/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.column;

import static io.openlineage.spark.agent.lifecycle.plan.column.TransformationInfo.Subtypes.CONDITIONAL;
import static io.openlineage.spark.agent.lifecycle.plan.column.TransformationInfo.Subtypes.FILTER;
import static io.openlineage.spark.agent.lifecycle.plan.column.TransformationInfo.Subtypes.GROUP_BY;
import static io.openlineage.spark.agent.lifecycle.plan.column.TransformationInfo.Subtypes.JOIN;
import static io.openlineage.spark.agent.lifecycle.plan.column.TransformationInfo.Subtypes.SORT;

import io.openlineage.spark.agent.lifecycle.plan.column.ColumnLevelLineageBuilder;
import io.openlineage.spark.agent.lifecycle.plan.column.ColumnLevelLineageContext;
import io.openlineage.spark.agent.lifecycle.plan.column.TransformationInfo;
import io.openlineage.spark.agent.util.ExtensionPlanUtils;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark3.agent.lifecycle.plan.column.visitors.ExpressionDependencyVisitor;
import io.openlineage.spark3.agent.lifecycle.plan.column.visitors.IcebergMergeIntoDependencyVisitor;
import io.openlineage.spark3.agent.lifecycle.plan.column.visitors.UnionDependencyVisitor;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.reflect.MethodUtils;
import org.apache.spark.sql.catalyst.expressions.Alias;
import org.apache.spark.sql.catalyst.expressions.AttributeReference;
import org.apache.spark.sql.catalyst.expressions.CaseWhen;
import org.apache.spark.sql.catalyst.expressions.ExprId;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.If;
import org.apache.spark.sql.catalyst.expressions.NamedExpression;
import org.apache.spark.sql.catalyst.expressions.SortOrder;
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression;
import org.apache.spark.sql.catalyst.plans.logical.Aggregate;
import org.apache.spark.sql.catalyst.plans.logical.Filter;
import org.apache.spark.sql.catalyst.plans.logical.Join;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.Project;
import org.apache.spark.sql.catalyst.plans.logical.Sort;
import org.apache.spark.sql.execution.datasources.LogicalRelation;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCRelation;
import scala.Option;
import scala.Tuple2;
import scala.collection.Seq;

/**
 * Traverses LogicalPlan and collects dependencies between the expressions and operations used
 * within the plan.
 */
@Slf4j
public class ExpressionDependencyCollector {

  private static final List<ExpressionDependencyVisitor> expressionDependencyVisitors =
      Arrays.asList(new UnionDependencyVisitor(), new IcebergMergeIntoDependencyVisitor());

  static void collect(ColumnLevelLineageContext context, LogicalPlan plan) {
    plan.foreach(
        node -> {
          collectFromNode(context, node);
          return scala.runtime.BoxedUnit.UNIT;
        });
  }

  static void collectFromNode(ColumnLevelLineageContext context, LogicalPlan node) {
    expressionDependencyVisitors.stream()
        .filter(collector -> collector.isDefinedAt(node))
        .forEach(collector -> collector.apply(node, context.getBuilder()));

    CustomCollectorsUtils.collectExpressionDependencies(context, node);
    List<NamedExpression> expressions = new LinkedList<>();
    List<Expression> datasetDependencies = new LinkedList<>();
    Optional<TransformationInfo> datasetTransformation = Optional.empty();

    if (node instanceof ColumnLevelLineageNode) {
      extensionColumnLineage(context, (ColumnLevelLineageNode) node);
    } else if (node instanceof Project) {
      expressions.addAll(
          ScalaConversionUtils.<NamedExpression>fromSeq(((Project) node).projectList()));
    } else if (node instanceof Aggregate) {
      datasetDependencies.addAll(
          ScalaConversionUtils.<Expression>fromSeq(((Aggregate) node).groupingExpressions()));
      datasetTransformation = Optional.of(TransformationInfo.indirect(GROUP_BY));
      expressions.addAll(
          ScalaConversionUtils.<NamedExpression>fromSeq(((Aggregate) node).aggregateExpressions()));
    } else if (node instanceof Join) {
      Option<Expression> condition = ((Join) node).condition();
      if (condition.isDefined()) {
        datasetTransformation = Optional.of(TransformationInfo.indirect(JOIN));
        datasetDependencies.add(condition.get());
      }
    } else if (node instanceof Filter) {
      datasetDependencies.add(((Filter) node).condition());
      datasetTransformation = Optional.of(TransformationInfo.indirect(FILTER));
    } else if (node instanceof Sort) {
      datasetDependencies.addAll(ScalaConversionUtils.<SortOrder>fromSeq(((Sort) node).order()));
      datasetTransformation = Optional.of(TransformationInfo.indirect(SORT));
    } else if (node instanceof LogicalRelation) {
      if (((LogicalRelation) node).relation() instanceof JDBCRelation) {
        JdbcColumnLineageCollector.extractExpressionsFromJDBC(context, node);
      }
    }
    expressions.stream()
        .forEach(
            expr ->
                traverseExpression(
                    (Expression) expr,
                    expr.exprId(),
                    TransformationInfo.identity(),
                    context.getBuilder()));

    datasetTransformation.ifPresent(
        dt -> {
          ExprId exprId = NamedExpression.newExprId();
          context.getBuilder().addDatasetDependency(exprId);
          datasetDependencies.forEach(e -> traverseExpression(e, exprId, dt, context.getBuilder()));
        });
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

    if (expr instanceof AttributeReference) {
      AttributeReference attRef = (AttributeReference) expr;
      if (!attRef.exprId().equals(outputExprId)) {
        builder.addDependency(outputExprId, attRef.exprId(), transformationInfo);
      }
    } else if (expr instanceof Alias) {
      handleAlias((Alias) expr, outputExprId, transformationInfo, builder);
    } else if (expr instanceof CaseWhen) {
      handleCaseWhen((CaseWhen) expr, outputExprId, transformationInfo, builder);
    } else if (expr instanceof If) {
      handleIf((If) expr, outputExprId, transformationInfo, builder);
    } else if (expr instanceof AggregateExpression) {
      handleAggregateExpression(
          (AggregateExpression) expr, outputExprId, transformationInfo, builder);
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
                    transformationInfo.merge(TransformationInfo.transformation()),
                    builder));
  }

  private static void handleAggregateExpression(
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

  private static void handleIf(
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

  private static void handleCaseWhen(
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

  private static void handleAlias(
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
