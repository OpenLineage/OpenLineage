/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark34.agent.lifecycle.plan.column;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.openlineage.spark.agent.lifecycle.plan.column.ColumnLevelLineageBuilder;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark3.agent.lifecycle.plan.column.InputFieldsCollector;
import io.openlineage.spark3.agent.lifecycle.plan.column.OutputFieldsCollector;
import java.util.Collections;
import java.util.Optional;
import org.apache.spark.sql.catalyst.expressions.AttributeReference;
import org.apache.spark.sql.catalyst.expressions.ExprId;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.plans.logical.DeltaMergeAction;
import org.apache.spark.sql.catalyst.plans.logical.DeltaMergeIntoMatchedClause;
import org.apache.spark.sql.catalyst.plans.logical.DeltaMergeIntoNotMatchedBySourceClause;
import org.apache.spark.sql.catalyst.plans.logical.DeltaMergeIntoNotMatchedClause;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.delta.commands.MergeIntoCommand;
import org.apache.spark.sql.delta.files.TahoeFileIndex;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import scala.Option;
import scala.collection.Seq$;

public class MergeIntoDelta24ColumnLineageVisitorTest {

  OpenLineageContext context = mock(OpenLineageContext.class);
  MergeIntoCommand command = mock(MergeIntoCommand.class);
  MergeIntoDelta24ColumnLineageVisitor visitor = new MergeIntoDelta24ColumnLineageVisitor(context);
  ColumnLevelLineageBuilder builder = mock(ColumnLevelLineageBuilder.class);
  DeltaMergeIntoMatchedClause deltaMergeIntoMatchedClause = mock(DeltaMergeIntoMatchedClause.class);
  DeltaMergeIntoNotMatchedClause deltaMergeIntoNotMatchedClause =
      mock(DeltaMergeIntoNotMatchedClause.class);
  DeltaMergeAction action1 = mock(DeltaMergeAction.class);
  AttributeReference actionChild1 = mock(AttributeReference.class);
  ExprId parentExprId1 = mock(ExprId.class);
  ExprId action1ExprId = mock(ExprId.class);
  DeltaMergeAction action2 = mock(DeltaMergeAction.class);
  AttributeReference actionChild2 = mock(AttributeReference.class);
  ExprId parentExprId2 = mock(ExprId.class);
  ExprId action2ExprId = mock(ExprId.class);

  @BeforeEach
  void setup() {
    when(deltaMergeIntoMatchedClause.actions())
        .thenReturn(ScalaConversionUtils.<Expression>fromList(Collections.singletonList(action1)));
    when(deltaMergeIntoNotMatchedClause.actions())
        .thenReturn(ScalaConversionUtils.<Expression>fromList(Collections.singletonList(action2)));
  }

  @Test
  void testCollectInputsIsCalled() {
    LogicalPlan source = mock(LogicalPlan.class);
    LogicalPlan target = mock(LogicalPlan.class);

    when(command.source()).thenReturn(source);
    when(command.target()).thenReturn(target);
    when(command.matchedClauses()).thenReturn(Seq$.MODULE$.empty());
    when(command.notMatchedClauses()).thenReturn(Seq$.MODULE$.empty());

    try (MockedStatic mocked = mockStatic(InputFieldsCollector.class)) {
      visitor.collectInputs(command, builder);
      mocked.verify(() -> InputFieldsCollector.collect(context, source, builder), times(1));
      mocked.verify(() -> InputFieldsCollector.collect(context, target, builder), times(1));
    }

    visitor.collectInputs(mock(LogicalPlan.class), builder);
  }

  @Test
  void testCollectOutputsIsCalled() {
    LogicalPlan source = mock(LogicalPlan.class);
    LogicalPlan target = mock(LogicalPlan.class);

    when(command.source()).thenReturn(source);
    when(command.target()).thenReturn(target);

    try (MockedStatic mocked = mockStatic(OutputFieldsCollector.class)) {
      visitor.collectOutputs(command, builder);
      mocked.verify(() -> OutputFieldsCollector.collect(context, source, builder), times(0));
      mocked.verify(() -> OutputFieldsCollector.collect(context, target, builder), times(1));
    }

    visitor.collectOutputs(mock(LogicalPlan.class), builder);
  }

  @Test
  void testGetMergeActions() {
    when(action1.targetColNameParts())
        .thenReturn(ScalaConversionUtils.<String>fromList(Collections.singletonList("col_1")));
    when(action2.targetColNameParts())
        .thenReturn(ScalaConversionUtils.<String>fromList(Collections.singletonList("col_2")));

    when(action1.child()).thenReturn(actionChild1);
    when(actionChild1.exprId()).thenReturn(action1ExprId);

    when(action2.child()).thenReturn(actionChild2);
    when(actionChild2.exprId()).thenReturn(action2ExprId);

    when(builder.getOutputExprIdByFieldName("col_1")).thenReturn(Optional.of(parentExprId1));
    when(builder.getOutputExprIdByFieldName("col_2")).thenReturn(Optional.of(parentExprId2));

    MergeIntoCommand command =
        new MergeIntoCommand(
            mock(LogicalPlan.class),
            mock(LogicalPlan.class),
            mock(TahoeFileIndex.class),
            mock(Expression.class),
            ScalaConversionUtils.<DeltaMergeIntoMatchedClause>fromList(
                Collections.singletonList(deltaMergeIntoMatchedClause)),
            ScalaConversionUtils.<DeltaMergeIntoNotMatchedClause>fromList(
                Collections.singletonList(deltaMergeIntoNotMatchedClause)),
            ScalaConversionUtils.<DeltaMergeIntoNotMatchedBySourceClause>fromList(
                Collections.emptyList()),
            Option.empty());

    visitor.collectExpressionDependencies(command, builder);

    verify(builder, times(1)).addDependency(parentExprId1, action1ExprId);
    verify(builder, times(1)).addDependency(parentExprId2, action2ExprId);
  }
}
