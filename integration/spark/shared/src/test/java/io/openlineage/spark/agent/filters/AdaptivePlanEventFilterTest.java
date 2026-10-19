/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.filters;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import io.openlineage.spark.agent.util.DatabricksUtils;
import io.openlineage.spark.api.OpenLineageContext;
import java.util.Optional;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.execution.QueryExecution;
import org.apache.spark.sql.execution.SparkPlan;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

class AdaptivePlanEventFilterTest {

  OpenLineageContext context = mock(OpenLineageContext.class);
  AdaptivePlanEventFilter filter = new AdaptivePlanEventFilter(context);
  SparkListenerEvent sparkListenerEvent = mock(SparkListenerEvent.class);
  QueryExecution queryExecution = mock(QueryExecution.class);
  SparkPlan sparkPlan = mock(SparkPlan.class);

  @BeforeEach
  public void setup() {
    when(context.getQueryExecution()).thenReturn(Optional.of(queryExecution));
    when(context.getCommandChildExecution()).thenReturn(Optional.empty());
    when(queryExecution.executedPlan()).thenReturn(sparkPlan);
  }

  @Test
  void testAdaptivePlanIsFiltered() {
    try (MockedStatic mocked = mockStatic(EventFilterUtils.class)) {
      when(EventFilterUtils.isDeltaPlan(context)).thenReturn(true);
      when(sparkPlan.nodeName()).thenReturn("AdaptiveSparkPlan");
      assertTrue(filter.isDisabled(sparkListenerEvent));
    }
  }

  @Test
  void testWhenQueryExecutionIsNull() {
    try (MockedStatic mocked = mockStatic(EventFilterUtils.class)) {
      when(EventFilterUtils.isDeltaPlan(context)).thenReturn(true);
      when(context.getQueryExecution()).thenReturn(Optional.ofNullable(null));
      assertFalse(filter.isDisabled(sparkListenerEvent));
    }
  }

  @Test
  void testWhenSparkPlanIsNull() {
    try (MockedStatic mocked = mockStatic(EventFilterUtils.class)) {
      when(EventFilterUtils.isDeltaPlan(context)).thenReturn(true);
      when(queryExecution.executedPlan()).thenReturn(null);
      assertFalse(filter.isDisabled(sparkListenerEvent));
    }
  }

  @Test
  void testOtherSparkPlan() {
    try (MockedStatic mocked = mockStatic(EventFilterUtils.class)) {
      when(EventFilterUtils.isDeltaPlan(context)).thenReturn(true);
      when(sparkPlan.nodeName()).thenReturn("OtherSparkPlan");
      assertFalse(filter.isDisabled(sparkListenerEvent));
    }
  }

  @Test
  void testNonDeltaPlan() {
    try (MockedStatic mocked = mockStatic(EventFilterUtils.class)) {
      when(EventFilterUtils.isDeltaPlan(context)).thenReturn(false);
      when(sparkPlan.nodeName()).thenReturn("AdaptiveSparkPlan");
      assertFalse(filter.isDisabled(sparkListenerEvent));
    }
  }

  @Test
  void testCommandChildIsFiltered() {
    when(context.getCommandChildExecution()).thenReturn(Optional.of(true));
    try (MockedStatic mocked = mockStatic(EventFilterUtils.class)) {
      when(EventFilterUtils.isDeltaPlan(context)).thenReturn(true);
      assertTrue(filter.isDisabled(sparkListenerEvent));
    }
  }

  @Test
  void testTopLevelAdaptivePlanIsNotFilteredWhenRootIsKnown() {
    when(context.getCommandChildExecution()).thenReturn(Optional.of(false));
    when(sparkPlan.nodeName()).thenReturn("AdaptiveSparkPlan");
    try (MockedStatic mocked = mockStatic(EventFilterUtils.class)) {
      when(EventFilterUtils.isDeltaPlan(context)).thenReturn(true);
      assertFalse(filter.isDisabled(sparkListenerEvent));
    }
  }

  @Test
  void testCommandChildIsFilteredOnDatabricksWithoutDeltaExtension() {
    when(context.getCommandChildExecution()).thenReturn(Optional.of(true));
    try (MockedStatic<EventFilterUtils> eventFilters = mockStatic(EventFilterUtils.class);
        MockedStatic<DatabricksUtils> databricks = mockStatic(DatabricksUtils.class)) {
      when(EventFilterUtils.isDeltaPlan(context)).thenReturn(false);
      databricks.when(() -> DatabricksUtils.isRunOnDatabricksPlatform(context)).thenReturn(true);
      assertTrue(filter.isDisabled(sparkListenerEvent));
    }
  }
}
