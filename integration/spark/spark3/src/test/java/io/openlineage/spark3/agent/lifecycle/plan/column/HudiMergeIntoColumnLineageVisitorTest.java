/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.column;

import static io.openlineage.spark3.agent.lifecycle.plan.column.ColumnLevelFixtures.asSeq;
import static io.openlineage.spark3.agent.lifecycle.plan.column.ColumnLevelFixtures.field;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import io.openlineage.spark.agent.lifecycle.plan.column.ColumnLevelLineageBuilder;
import io.openlineage.spark.agent.lifecycle.plan.column.ColumnLevelLineageContext;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.spark.sql.catalyst.expressions.AttributeReference;
import org.apache.spark.sql.catalyst.expressions.ExprId;
import org.apache.spark.sql.catalyst.plans.logical.Assignment;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.MergeAction;
import org.apache.spark.sql.catalyst.plans.logical.MergeIntoTable;
import org.apache.spark.sql.catalyst.plans.logical.UpdateAction;
import org.apache.spark.sql.hudi.command.MergeIntoHoodieTableCommand;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

class HudiMergeIntoColumnLineageVisitorTest {

  private static final ExprId TARGET_NAME = ExprId.apply(1);
  private static final ExprId TARGET_ID = ExprId.apply(2);
  private static final ExprId TARGET_PREV_NAME = ExprId.apply(3);
  private static final ExprId SOURCE_NAME = ExprId.apply(4);
  private static final ExprId EARLIER_INPUT = ExprId.apply(5);

  ColumnLevelLineageContext context = mock(ColumnLevelLineageContext.class);
  ColumnLevelLineageBuilder builder = mock(ColumnLevelLineageBuilder.class);
  Map<ExprId, Object> inputs = new HashMap<>();

  @Test
  void testTargetColumnsReadByAssignmentsAreInputs() {
    // MERGE INTO t USING s ON t.id = s.id
    // WHEN MATCHED THEN UPDATE SET prev_name = t.name, name = s.name
    AttributeReference targetName = field("name", TARGET_NAME);
    AttributeReference targetPrevName = field("prev_name", TARGET_PREV_NAME);
    UpdateAction update = mock(UpdateAction.class);
    when(update.assignments())
        .thenReturn(
            asSeq(
                new Assignment(targetPrevName, targetName),
                new Assignment(targetName, field("name", SOURCE_NAME))));
    LogicalPlan target = mock(LogicalPlan.class);
    MergeIntoTable merge = mock(MergeIntoTable.class);
    when(merge.targetTable()).thenReturn(target);
    when(merge.matchedActions()).thenReturn(asSeq((MergeAction) update));
    when(merge.notMatchedActions()).thenReturn(ScalaConversionUtils.asScalaSeqEmpty());
    MergeIntoHoodieTableCommand command = mock(MergeIntoHoodieTableCommand.class);
    when(command.mergeInto()).thenReturn(merge);
    when(context.getBuilder()).thenReturn(builder);
    doReturn(inputs).when(builder).getInputs();
    inputs.put(EARLIER_INPUT, Collections.emptySet());

    try (MockedStatic<InputFieldsCollector> collector = mockStatic(InputFieldsCollector.class)) {
      collector
          .when(() -> InputFieldsCollector.collect(any(), any()))
          .thenAnswer(
              invocation -> {
                // the target's columns
                inputs.put(TARGET_NAME, Collections.emptySet());
                inputs.put(TARGET_ID, Collections.emptySet());
                inputs.put(TARGET_PREV_NAME, Collections.emptySet());
                return null;
              });

      new HudiMergeIntoColumnLineageVisitor().collectInputs(context, command);
    }

    assertThat(inputs).containsOnlyKeys(TARGET_NAME, EARLIER_INPUT);
  }

  @Test
  void testOtherPlansAddNoInputs() {
    when(context.getBuilder()).thenReturn(builder);
    doReturn(inputs).when(builder).getInputs();

    new HudiMergeIntoColumnLineageVisitor().collectInputs(context, mock(LogicalPlan.class));

    assertThat(inputs).isEmpty();
  }
}
