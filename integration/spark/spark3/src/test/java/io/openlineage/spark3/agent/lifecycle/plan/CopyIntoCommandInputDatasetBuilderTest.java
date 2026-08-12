/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.InputDataset;
import io.openlineage.spark.agent.Versions;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark3.agent.utils.CopyIntoCommandUtils;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.catalyst.plans.logical.AppendData;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.OneRowRelation;
import org.apache.spark.sql.catalyst.plans.logical.Project;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import scala.PartialFunction;
import scala.runtime.AbstractPartialFunction;

class CopyIntoCommandInputDatasetBuilderTest {

  private static final OpenLineage OPEN_LINEAGE =
      new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI);

  OpenLineageContext context = mock(OpenLineageContext.class);
  CopyIntoCommandInputDatasetBuilder builder = new CopyIntoCommandInputDatasetBuilder(context);
  SparkListenerEvent event = new SparkListenerSQLExecutionEnd(1L, 1L);

  @Test
  void testIsNotDefinedForNonCopyIntoPlans() {
    assertFalse(builder.isDefinedAtLogicalPlan(mock(LogicalPlan.class)));
    assertFalse(builder.isDefinedAtLogicalPlan(mock(AppendData.class)));
  }

  @Test
  void testApplyDelegatesWhenOnlySourceQueryIsPresent() {
    LogicalPlan command = mock(LogicalPlan.class);
    OneRowRelation nestedRelation = new OneRowRelation();
    LogicalPlan sourceQuery = new Project(ScalaConversionUtils.asScalaSeqEmpty(), nestedRelation);
    givenInputVisitorReturning(OneRowRelation.class, "source_table", "unity-catalog");

    try (MockedStatic<CopyIntoCommandUtils> utils = mockStatic(CopyIntoCommandUtils.class)) {
      utils.when(() -> CopyIntoCommandUtils.isCopyIntoCommand(command)).thenReturn(true);
      utils.when(() -> CopyIntoCommandUtils.sourcePath(command)).thenReturn(Optional.empty());
      utils
          .when(() -> CopyIntoCommandUtils.sourceQuery(command))
          .thenReturn(Optional.of(sourceQuery));

      List<InputDataset> inputs = builder.apply(event, command);

      assertThat(inputs)
          .singleElement()
          .hasFieldOrPropertyWithValue("name", "source_table")
          .hasFieldOrPropertyWithValue("namespace", "unity-catalog");
    }
  }

  private void givenInputVisitorReturning(
      Class<? extends LogicalPlan> planType, String name, String namespace) {
    PartialFunction<LogicalPlan, List<InputDataset>> visitor =
        new AbstractPartialFunction<LogicalPlan, List<InputDataset>>() {
          @Override
          public boolean isDefinedAt(LogicalPlan x) {
            return planType.isInstance(x);
          }

          @Override
          public List<InputDataset> apply(LogicalPlan x) {
            return Collections.singletonList(
                OPEN_LINEAGE.newInputDatasetBuilder().name(name).namespace(namespace).build());
          }
        };

    when(context.getInputDatasetQueryPlanVisitors()).thenReturn(Collections.singletonList(visitor));
    when(context.getInputDatasetBuilders()).thenReturn(Collections.emptyList());
  }
}
