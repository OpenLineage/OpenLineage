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
import io.openlineage.client.OpenLineage.OutputDataset;
import io.openlineage.spark.agent.Versions;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark3.agent.utils.UpdateCommandUtils;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.OneRowRelation;
import org.apache.spark.sql.catalyst.plans.logical.UpdateTable;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import scala.PartialFunction;
import scala.runtime.AbstractPartialFunction;

class UpdateCommandOutputDatasetBuilderTest {

  private static final OpenLineage OPEN_LINEAGE =
      new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI);

  OpenLineageContext context = mock(OpenLineageContext.class);
  UpdateCommandOutputDatasetBuilder builder = new UpdateCommandOutputDatasetBuilder(context);
  SparkListenerEvent event = new SparkListenerSQLExecutionEnd(1L, 1L);

  @Test
  void testIsNotDefinedAtOtherPlans() {
    assertFalse(builder.isDefinedAtLogicalPlan(new OneRowRelation()));
    assertFalse(builder.isDefinedAtLogicalPlan(mock(LogicalPlan.class)));
    assertFalse(builder.isDefinedAtLogicalPlan(mock(UpdateTable.class)));
  }

  @Test
  void testApplyEmitsTargetAsOutput() {
    givenTargetVisitorReturning("update_table", "unity-catalog");
    LogicalPlan command = mock(LogicalPlan.class);

    try (MockedStatic<UpdateCommandUtils> utils = mockStatic(UpdateCommandUtils.class)) {
      utils
          .when(() -> UpdateCommandUtils.target(command))
          .thenReturn(Optional.of(new OneRowRelation()));

      List<OutputDataset> outputs = builder.apply(event, command);

      assertThat(outputs)
          .singleElement()
          .hasFieldOrPropertyWithValue("name", "update_table")
          .hasFieldOrPropertyWithValue("namespace", "unity-catalog");
    }
  }

  @Test
  void testApplyWhenTargetIsMissing() {
    LogicalPlan command = mock(LogicalPlan.class);

    try (MockedStatic<UpdateCommandUtils> utils = mockStatic(UpdateCommandUtils.class)) {
      utils.when(() -> UpdateCommandUtils.target(command)).thenReturn(Optional.empty());

      assertThat(builder.apply(event, command)).isEmpty();
    }
  }

  private void givenTargetVisitorReturning(String name, String namespace) {
    PartialFunction<LogicalPlan, List<OutputDataset>> visitor =
        new AbstractPartialFunction<LogicalPlan, List<OutputDataset>>() {
          @Override
          public boolean isDefinedAt(LogicalPlan x) {
            return x instanceof OneRowRelation;
          }

          @Override
          public List<OutputDataset> apply(LogicalPlan x) {
            return Collections.singletonList(
                OPEN_LINEAGE.newOutputDatasetBuilder().name(name).namespace(namespace).build());
          }
        };

    when(context.getOutputDatasetQueryPlanVisitors())
        .thenReturn(Collections.singletonList(visitor));
    when(context.getOutputDatasetBuilders()).thenReturn(Collections.emptyList());
  }
}
