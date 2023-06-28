/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark32.agent.lifecycle.plan.column;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark3.agent.lifecycle.plan.column.ColumnLevelLineageBuilder;
import java.util.Collections;
import java.util.Optional;
import org.apache.spark.sql.catalyst.expressions.AttributeReference;
import org.apache.spark.sql.catalyst.expressions.ExprId;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.plans.logical.DeltaMergeAction;
import org.apache.spark.sql.catalyst.plans.logical.DeltaMergeIntoInsertClause;
import org.apache.spark.sql.catalyst.plans.logical.DeltaMergeIntoMatchedClause;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.delta.commands.MergeIntoCommand;
import org.apache.spark.sql.delta.files.TahoeFileIndex;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import scala.Option;

public class MergeIntoDeltaColumnLineageVisitorTest {
  OpenLineageContext context = mock(OpenLineageContext.class);
  MergeIntoCommand command = mock(MergeIntoCommand.class);
  MergeIntoDeltaColumnLineageVisitor visitor = new MergeIntoDeltaColumnLineageVisitor(context);
  ColumnLevelLineageBuilder builder = mock(ColumnLevelLineageBuilder.class);

  @Test
  void testGetMergeActions() {
    DeltaMergeIntoMatchedClause deltaMergeIntoMatchedClause =
        mock(DeltaMergeIntoMatchedClause.class);
    DeltaMergeIntoInsertClause deltaMergeIntoNotMatchedClause =
        mock(DeltaMergeIntoInsertClause.class);

    DeltaMergeAction action1 = mock(DeltaMergeAction.class);
    AttributeReference actionChild1 = mock(AttributeReference.class);
    ExprId parentExprId1 = mock(ExprId.class);
    ExprId action1ExprId = mock(ExprId.class);

    DeltaMergeAction action2 = mock(DeltaMergeAction.class);
    AttributeReference actionChild2 = mock(AttributeReference.class);
    ExprId parentExprId2 = mock(ExprId.class);
    ExprId action2ExprId = mock(ExprId.class);

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

    when(deltaMergeIntoMatchedClause.actions())
        .thenReturn(ScalaConversionUtils.<Expression>fromList(Collections.singletonList(action1)));
    when(deltaMergeIntoNotMatchedClause.actions())
        .thenReturn(ScalaConversionUtils.<Expression>fromList(Collections.singletonList(action2)));

    MergeIntoCommand command =
        new MergeIntoCommand(
            mock(LogicalPlan.class),
            mock(LogicalPlan.class),
            mock(TahoeFileIndex.class),
            mock(Expression.class),
            ScalaConversionUtils.<DeltaMergeIntoMatchedClause>fromList(
                Collections.singletonList(deltaMergeIntoMatchedClause)),
            ScalaConversionUtils.<DeltaMergeIntoInsertClause>fromList(
                Collections.singletonList(deltaMergeIntoNotMatchedClause)),
            Option.<StructType>empty());

    visitor.collectExpressionDependencies(command, builder);

    verify(builder, times(1)).addDependency(parentExprId1, action1ExprId);
    verify(builder, times(1)).addDependency(parentExprId2, action2ExprId);
  }
}
