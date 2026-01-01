/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.spark34.agent.lifecycle.plan.column;

import io.openlineage.spark.agent.lifecycle.plan.column.ColumnLevelLineageContext;
import io.openlineage.spark.agent.lifecycle.plan.column.ColumnLevelLineageVisitor;
import io.openlineage.spark.agent.util.ReflectionUtils;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark3.agent.lifecycle.plan.column.InputFieldsCollector;
import io.openlineage.spark3.agent.lifecycle.plan.column.OutputFieldsCollector;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.reflect.MethodUtils;
import org.apache.spark.sql.catalyst.expressions.AttributeReference;
import org.apache.spark.sql.catalyst.expressions.ExprId;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.plans.logical.DeltaMergeAction;
import org.apache.spark.sql.catalyst.plans.logical.DeltaMergeIntoMatchedClause;
import org.apache.spark.sql.catalyst.plans.logical.DeltaMergeIntoNotMatchedClause;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import scala.collection.mutable.ArrayBuffer;

/**
 * Builder to extract column level lineage from MergeIntoCommandEdge available only on Databricks
 * runtime
 */
@Slf4j
public class MergeIntoCommandEdgeColumnLineageBuilder implements ColumnLevelLineageVisitor {

  protected OpenLineageContext context;
  private static String CLASS = "sql.transaction.tahoe.commands.MergeIntoCommandEdge";

  public static boolean hasClasses() {
    return ReflectionUtils.hasClasses(
        "com.databricks.sql.transaction.tahoe.commands.MergeIntoCommandEdge",
        "org.apache.spark.sql.catalyst.plans.logical.DeltaMergeIntoNotMatchedClause");
  }

  public MergeIntoCommandEdgeColumnLineageBuilder(OpenLineageContext context) {
    this.context = context;
  }

  @Override
  public void collectInputs(ColumnLevelLineageContext context, LogicalPlan node) {
    if (!node.getClass().getCanonicalName().endsWith(CLASS)) return;

    this.<LogicalPlan>getFieldFromNode(node, "target")
        .ifPresent(target -> InputFieldsCollector.collect(context, target));

    // remove builder target inputs that are not contained within merge actions
    List<ExprId> mergeActionsExprIds =
        getMergeActions(node)
            .filter(action -> action instanceof DeltaMergeAction)
            .map(action -> (DeltaMergeAction) action)
            .filter(action -> action.child() instanceof AttributeReference)
            .filter(
                action ->
                    context
                        .getBuilder()
                        .getOutputExprIdByFieldName(action.targetColNameParts().mkString())
                        .isPresent())
            .map(action -> ((AttributeReference) action.child()).exprId())
            .collect(Collectors.toList());

    List<ExprId> inputsToRemove =
        context.getBuilder().getInputs().keySet().stream()
            .filter(id -> !mergeActionsExprIds.contains(id))
            .collect(Collectors.toList());

    inputsToRemove.forEach(id -> context.getBuilder().getInputs().remove(id));

    this.<LogicalPlan>getFieldFromNode(node, "source")
        .ifPresent(source -> InputFieldsCollector.collect(context, source));
  }

  @Override
  public void collectOutputs(ColumnLevelLineageContext context, LogicalPlan node) {
    if (!node.getClass().getCanonicalName().endsWith(CLASS)) return;

    this.<LogicalPlan>getFieldFromNode(node, "target")
        .ifPresent(target -> OutputFieldsCollector.collect(context, target));
  }

  @Override
  public void collectExpressionDependencies(ColumnLevelLineageContext context, LogicalPlan node) {
    if (!node.getClass().getCanonicalName().endsWith(CLASS)) return;

    getMergeActions(node)
        .filter(action -> action instanceof DeltaMergeAction)
        .map(action -> (DeltaMergeAction) action)
        .filter(action -> action.child() instanceof AttributeReference)
        .filter(
            action ->
                context
                    .getBuilder()
                    .getOutputExprIdByFieldName(action.targetColNameParts().mkString())
                    .isPresent())
        .forEach(
            action ->
                context
                    .getBuilder()
                    .addDependency(
                        context
                            .getBuilder()
                            .getOutputExprIdByFieldName(action.targetColNameParts().mkString())
                            .get(),
                        ((AttributeReference) action.child()).exprId()));
  }

  public Stream<Expression> getMergeActions(LogicalPlan node) {
    Optional<ArrayBuffer<DeltaMergeIntoMatchedClause>> matchedClauses;
    Optional<ArrayBuffer<DeltaMergeIntoNotMatchedClause>> notMatchedClauses;

    matchedClauses = this.getFieldFromNode(node, "matchedClauses");
    notMatchedClauses = this.getFieldFromNode(node, "notMatchedClauses");

    return Stream.concat(
        ScalaConversionUtils.<DeltaMergeIntoMatchedClause>fromSeq(
                matchedClauses.orElse(new ArrayBuffer<>()))
            .stream()
            .flatMap(clause -> ScalaConversionUtils.<Expression>fromSeq(clause.actions()).stream()),
        ScalaConversionUtils.<DeltaMergeIntoNotMatchedClause>fromSeq(
                notMatchedClauses.orElse(
                    new ArrayBuffer<>())) // DeltaMergeIntoNotMatchedClause class does not
            // exist in earlier versions
            .stream()
            .flatMap(
                clause -> ScalaConversionUtils.<Expression>fromSeq(clause.actions()).stream()));
  }

  private <T> Optional<T> getFieldFromNode(LogicalPlan node, String field) {
    try {
      return Optional.of((T) MethodUtils.invokeMethod(node, field));
    } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
      log.warn("Couldn't extract field {} from {}", field, node);
    }

    return Optional.empty();
  }
}
