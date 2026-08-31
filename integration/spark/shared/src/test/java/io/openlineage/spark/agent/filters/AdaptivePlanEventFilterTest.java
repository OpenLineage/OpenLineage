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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

class AdaptivePlanEventFilterTest {

  private final OpenLineageContext context = mock(OpenLineageContext.class);
  private final AdaptivePlanEventFilter filter = new AdaptivePlanEventFilter(context);
  private final SparkListenerEvent sparkListenerEvent = mock(SparkListenerEvent.class);
  private final QueryExecution queryExecution = mock(QueryExecution.class);

  @BeforeEach
  public void setup() {
    when(context.getQueryExecution()).thenReturn(Optional.of(queryExecution));
    when(context.isCommandChildExecution()).thenReturn(true);
  }

  @Test
  void testNestedExecutionIsFiltered() {
    try (MockedStatic<EventFilterUtils> filters = mockStatic(EventFilterUtils.class)) {
      filters.when(() -> EventFilterUtils.isDeltaPlan(context)).thenReturn(true);
      assertTrue(filter.isDisabled(sparkListenerEvent));
    }
  }

  @Test
  void testTopLevelExecutionIsNotFiltered() {
    when(context.isCommandChildExecution()).thenReturn(false);
    assertFalse(filter.isDisabled(sparkListenerEvent));
  }

  @Test
  void testWhenQueryExecutionIsNull() {
    try (MockedStatic<EventFilterUtils> filters = mockStatic(EventFilterUtils.class)) {
      filters.when(() -> EventFilterUtils.isDeltaPlan(context)).thenReturn(true);
      when(context.getQueryExecution()).thenReturn(Optional.empty());
      assertFalse(filter.isDisabled(sparkListenerEvent));
    }
  }

  @Test
  void testNestedExecutionDoesNotRequireAnAdaptivePlan() {
    try (MockedStatic<EventFilterUtils> filters = mockStatic(EventFilterUtils.class)) {
      filters.when(() -> EventFilterUtils.isDeltaPlan(context)).thenReturn(true);
      when(queryExecution.executedPlan()).thenReturn(null);
      assertTrue(filter.isDisabled(sparkListenerEvent));
    }
  }

  @Test
  void testChildOfNonCommandRootIsNotFiltered() {
    when(context.isCommandChildExecution()).thenReturn(false);
    assertFalse(filter.isDisabled(sparkListenerEvent));
  }

  @Test
  void testCommandChildOutsideDeltaEnvironmentIsNotFiltered() {
    try (MockedStatic<EventFilterUtils> filters = mockStatic(EventFilterUtils.class);
        MockedStatic<DatabricksUtils> databricks = mockStatic(DatabricksUtils.class)) {
      filters.when(() -> EventFilterUtils.isDeltaPlan(context)).thenReturn(false);
      databricks.when(() -> DatabricksUtils.isRunOnDatabricksPlatform(context)).thenReturn(false);
      assertFalse(filter.isDisabled(sparkListenerEvent));
    }
  }

  @Test
  void testNestedDatabricksCommandIsFiltered() {
    try (MockedStatic<EventFilterUtils> filters = mockStatic(EventFilterUtils.class);
        MockedStatic<DatabricksUtils> databricks = mockStatic(DatabricksUtils.class)) {
      filters.when(() -> EventFilterUtils.isDeltaPlan(context)).thenReturn(false);
      databricks.when(() -> DatabricksUtils.isRunOnDatabricksPlatform(context)).thenReturn(true);
      assertTrue(filter.isDisabled(sparkListenerEvent));
    }
  }
}
