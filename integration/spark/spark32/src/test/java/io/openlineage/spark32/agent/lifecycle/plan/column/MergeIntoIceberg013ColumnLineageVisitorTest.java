/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark32.agent.lifecycle.plan.column;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import io.openlineage.spark.agent.lifecycle.plan.column.ColumnLevelLineageBuilder;
import io.openlineage.spark.agent.lifecycle.plan.column.ColumnLevelLineageContext;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark3.agent.lifecycle.plan.column.InputFieldsCollector;
import io.openlineage.spark3.agent.lifecycle.plan.column.OutputFieldsCollector;
import java.util.Collections;
import org.apache.spark.sql.catalyst.analysis.NamedRelation;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.expressions.ExprId;
import org.apache.spark.sql.catalyst.expressions.NamedExpression;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.Project;
import org.apache.spark.sql.catalyst.plans.logical.ReplaceData;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

class MergeIntoIceberg013ColumnLineageVisitorTest {

  OpenLineageContext olContext = mock(OpenLineageContext.class);
  ColumnLevelLineageContext clContext = mock(ColumnLevelLineageContext.class);
  ReplaceData replaceIcebergData = mock(ReplaceData.class);
  MergeIntoIceberg013ColumnLineageVisitor visitor =
      new MergeIntoIceberg013ColumnLineageVisitor(olContext);
  ColumnLevelLineageBuilder builder = mock(ColumnLevelLineageBuilder.class);
  private LogicalPlan target =
      mock(LogicalPlan.class, withSettings().extraInterfaces(NamedRelation.class));
  private Project project = mock(Project.class);

  @BeforeEach
  public void setup() {
    when(clContext.getBuilder()).thenReturn(builder);
    when(clContext.getOlContext()).thenReturn(olContext);

    when(replaceIcebergData.query()).thenReturn(project);
    when(replaceIcebergData.table()).thenReturn((NamedRelation) target);
  }

  @Test
  @SuppressWarnings("PMD.JUnitTestsShouldIncludeAssert")
  void testCollectInputsIsCalled() {
    LogicalPlan source = mock(LogicalPlan.class);
    LogicalPlan target =
        mock(LogicalPlan.class, withSettings().extraInterfaces(NamedRelation.class));

    when(replaceIcebergData.child()).thenReturn(source);
    when(replaceIcebergData.table()).thenReturn((NamedRelation) target);

    try (MockedStatic mocked = mockStatic(InputFieldsCollector.class)) {
      visitor.collectInputs(clContext, replaceIcebergData);
      mocked.verify(() -> InputFieldsCollector.collect(clContext, source), times(1));
      mocked.verify(() -> InputFieldsCollector.collect(clContext, target), times(1));
    }

    visitor.collectInputs(clContext, mock(LogicalPlan.class));
  }

  @Test
  @SuppressWarnings("PMD.JUnitTestsShouldIncludeAssert")
  void testCollectOutputsIsCalled() {
    LogicalPlan source = mock(LogicalPlan.class);
    LogicalPlan target =
        mock(LogicalPlan.class, withSettings().extraInterfaces(NamedRelation.class));

    when(replaceIcebergData.child()).thenReturn(source);
    when(replaceIcebergData.table()).thenReturn((NamedRelation) target);

    try (MockedStatic mocked = mockStatic(OutputFieldsCollector.class)) {
      visitor.collectOutputs(clContext, replaceIcebergData);
      mocked.verify(() -> OutputFieldsCollector.collect(clContext, source), times(0));
      mocked.verify(() -> OutputFieldsCollector.collect(clContext, target), times(1));
    }

    visitor.collectInputs(clContext, mock(LogicalPlan.class));
  }

  @Test
  void testGetMergeActions() {
    ExprId projectExprId = mock(ExprId.class);
    ExprId tableExprId = mock(ExprId.class);

    NamedExpression projectNamedExpression = mock(NamedExpression.class);
    when(projectNamedExpression.exprId()).thenReturn(projectExprId);

    when(project.projectList())
        .thenReturn(
            ScalaConversionUtils.<NamedExpression>fromList(
                Collections.singletonList(projectNamedExpression)));

    Attribute tableAttribute = mock(Attribute.class);
    when(tableAttribute.exprId()).thenReturn(tableExprId);
    when(target.output())
        .thenReturn(
            ScalaConversionUtils.<Attribute>fromList(Collections.singletonList(tableAttribute)));

    visitor.collectExpressionDependencies(clContext, replaceIcebergData);
    verify(builder, times(1)).addDependency(tableExprId, projectExprId);
  }

  @Test
  void testGetMergeActionsWhenProjectListDiffers() {
    ExprId projectExprId = mock(ExprId.class);
    ExprId tableExprId = mock(ExprId.class);

    NamedExpression projectNamedExpression = mock(NamedExpression.class);
    when(projectNamedExpression.exprId()).thenReturn(projectExprId);

    when(project.projectList())
        .thenReturn(
            ScalaConversionUtils.<NamedExpression>fromList(
                Collections.emptyList() // no outputs in project list
                ));

    Attribute tableAttribute = mock(Attribute.class);
    when(tableAttribute.exprId()).thenReturn(tableExprId);
    when(target.output())
        .thenReturn(
            ScalaConversionUtils.<Attribute>fromList(Collections.singletonList(tableAttribute)));

    visitor.collectExpressionDependencies(clContext, replaceIcebergData);
    verify(builder, times(0)).addDependency(any(), any());
  }
}
