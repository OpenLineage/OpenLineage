/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.column;

import io.openlineage.spark.agent.lifecycle.plan.column.ColumnLevelLineageBuilder;
import io.openlineage.spark.agent.lifecycle.plan.column.ColumnLevelLineageVisitor;
import io.openlineage.spark.api.OpenLineageContext;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.spark.sql.catalyst.expressions.AttributeReference;
import org.apache.spark.sql.catalyst.expressions.ExprId;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.plans.logical.DeltaMergeAction;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.delta.commands.MergeIntoCommand;

public abstract class MergeIntoDeltaColumnLineageVisitor implements ColumnLevelLineageVisitor {
  protected OpenLineageContext context;

  public MergeIntoDeltaColumnLineageVisitor(OpenLineageContext context) {
    this.context = context;
  }

  @Override
  public void collectInputs(LogicalPlan node, ColumnLevelLineageBuilder builder) {
    if (node instanceof MergeIntoCommand) {
      InputFieldsCollector.collect(context, ((MergeIntoCommand) node).target(), builder);

      List<Expression> mergeActions =
          getMergeActions((MergeIntoCommand) node).collect(Collectors.toList());

      // remove builder target inputs that are not contained within merge actions
      List<ExprId> mergeActionsExprIds =
          getMergeActions((MergeIntoCommand) node)
              .filter(action -> action instanceof DeltaMergeAction)
              .map(action -> (DeltaMergeAction) action)
              .filter(action -> action.child() instanceof AttributeReference)
              .filter(
                  action ->
                      builder
                          .getOutputExprIdByFieldName(action.targetColNameParts().mkString())
                          .isPresent())
              .map(action -> ((AttributeReference) action.child()).exprId())
              .collect(Collectors.toList());

      List<ExprId> inputsToRemove =
          builder.getInputs().keySet().stream()
              .filter(id -> !mergeActionsExprIds.contains(id))
              .collect(Collectors.toList());

      inputsToRemove.forEach(id -> builder.getInputs().remove(id));

      InputFieldsCollector.collect(context, ((MergeIntoCommand) node).source(), builder);
    }
  }

  @Override
  public void collectOutputs(LogicalPlan node, ColumnLevelLineageBuilder builder) {
    if (node instanceof MergeIntoCommand) {
      OutputFieldsCollector.collect(context, ((MergeIntoCommand) node).target(), builder);
    }
  }

  public abstract Stream<Expression> getMergeActions(MergeIntoCommand mergeIntoCommand);

  @Override
  public void collectExpressionDependencies(LogicalPlan node, ColumnLevelLineageBuilder builder) {
    if (node instanceof MergeIntoCommand) {
      getMergeActions((MergeIntoCommand) node)
          .filter(action -> action instanceof DeltaMergeAction)
          .map(action -> (DeltaMergeAction) action)
          .filter(action -> action.child() instanceof AttributeReference)
          .filter(
              action ->
                  builder
                      .getOutputExprIdByFieldName(action.targetColNameParts().mkString())
                      .isPresent())
          .forEach(
              action ->
                  builder.addDependency(
                      builder
                          .getOutputExprIdByFieldName(action.targetColNameParts().mkString())
                          .get(),
                      ((AttributeReference) action.child()).exprId()));
    }
  }

  private Stream<DeltaMergeAction> getMergeActionsAttributes(
      LogicalPlan node, ColumnLevelLineageBuilder builder) {
    return getMergeActions((MergeIntoCommand) node)
        .filter(action -> action instanceof DeltaMergeAction)
        .map(action -> (DeltaMergeAction) action)
        .filter(action -> action.child() instanceof AttributeReference)
        .filter(
            action ->
                builder
                    .getOutputExprIdByFieldName(action.targetColNameParts().mkString())
                    .isPresent());
  }
}
