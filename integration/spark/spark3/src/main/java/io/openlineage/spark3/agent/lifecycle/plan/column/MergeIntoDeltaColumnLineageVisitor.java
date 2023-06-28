/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.column;

import io.openlineage.spark.api.OpenLineageContext;
import java.util.stream.Stream;
import org.apache.spark.sql.catalyst.expressions.AttributeReference;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.plans.logical.DeltaMergeAction;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.delta.commands.MergeIntoCommand;

public abstract class MergeIntoDeltaColumnLineageVisitor implements CustomColumnLineageVisitor {
  protected OpenLineageContext context;

  public MergeIntoDeltaColumnLineageVisitor(OpenLineageContext context) {
    this.context = context;
  }

  @Override
  public void collectInputs(LogicalPlan node, ColumnLevelLineageBuilder builder) {
    if (node instanceof MergeIntoCommand) {
      InputFieldsCollector.collect(context, ((MergeIntoCommand) node).source(), builder);
      InputFieldsCollector.collect(context, ((MergeIntoCommand) node).target(), builder);
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
}
