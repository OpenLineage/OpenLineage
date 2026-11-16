/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.column;

import io.openlineage.spark.agent.lifecycle.plan.column.ColumnLevelLineageContext;
import io.openlineage.spark.agent.lifecycle.plan.column.ColumnLevelLineageVisitor;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark3.agent.utils.HudiUtils;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.reflect.MethodUtils;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.plans.logical.Assignment;
import org.apache.spark.sql.catalyst.plans.logical.InsertAction;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.MergeAction;
import org.apache.spark.sql.catalyst.plans.logical.MergeIntoTable;
import org.apache.spark.sql.catalyst.plans.logical.UpdateAction;
import scala.collection.Seq;

/**
 * Column lineage of Hudi's {@code MergeIntoHoodieTableCommand}. The command is a leaf: the merge it
 * runs is a Spark {@code MergeIntoTable} kept as a field, so its target, source and actions are
 * read from there. Each target column set by an {@code UPDATE SET} or {@code INSERT VALUES}
 * assignment depends on the columns its expression reads.
 */
@Slf4j
public class HudiMergeIntoColumnLineageVisitor implements ColumnLevelLineageVisitor {

  @Override
  public void collectInputs(ColumnLevelLineageContext context, LogicalPlan node) {
    // the source's inputs are collected with its expression dependencies
  }

  @Override
  public void collectOutputs(ColumnLevelLineageContext context, LogicalPlan node) {
    if (HudiUtils.isMergeIntoHoodieTableCommand(node)) {
      HudiUtils.dmlTarget(node).ifPresent(target -> OutputFieldsCollector.collect(context, target));
    }
  }

  @Override
  public void collectExpressionDependencies(ColumnLevelLineageContext context, LogicalPlan node) {
    if (!HudiUtils.isMergeIntoHoodieTableCommand(node)) {
      return;
    }
    HudiUtils.mergeInto(node)
        .ifPresent(
            merge -> {
              ColumnLevelLineageUtils.collectInputsAndExpressionDependencies(
                  context, merge.sourceTable());
              assignments(merge).forEach(assignment -> addDependencies(context, assignment));
            });
  }

  private static void addDependencies(ColumnLevelLineageContext context, Assignment assignment) {
    if (!(assignment.key() instanceof Attribute)) {
      return;
    }
    context
        .getBuilder()
        .getOutputExprIdByFieldName(((Attribute) assignment.key()).name())
        .ifPresent(
            output ->
                ExpressionTraverser.of(assignment.value(), output, context.getBuilder())
                    .traverse());
  }

  private static Stream<Assignment> assignments(MergeIntoTable merge) {
    List<MergeAction> actions = new ArrayList<>();
    actions.addAll(ScalaConversionUtils.fromSeq(merge.matchedActions()));
    actions.addAll(ScalaConversionUtils.fromSeq(merge.notMatchedActions()));
    actions.addAll(notMatchedBySourceActions(merge));
    return actions.stream()
        .flatMap(
            action -> {
              if (action instanceof UpdateAction) {
                return ScalaConversionUtils.fromSeq(((UpdateAction) action).assignments()).stream();
              } else if (action instanceof InsertAction) {
                return ScalaConversionUtils.fromSeq(((InsertAction) action).assignments()).stream();
              }
              return Stream.empty();
            });
  }

  /** {@code WHEN NOT MATCHED BY SOURCE} actions, which Spark has from 3.4 only. */
  @SuppressWarnings("unchecked")
  private static List<MergeAction> notMatchedBySourceActions(MergeIntoTable merge) {
    try {
      Object actions = MethodUtils.invokeMethod(merge, "notMatchedBySourceActions");
      if (actions instanceof Seq) {
        return ScalaConversionUtils.fromSeq((Seq<MergeAction>) actions);
      }
    } catch (ReflectiveOperationException e) {
      log.debug("MergeIntoTable has no notMatchedBySourceActions", e);
    }
    return new ArrayList<>();
  }
}
