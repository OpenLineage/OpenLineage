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

import io.openlineage.spark.api.OpenLineageContext;
import java.util.Optional;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.execution.QueryExecution;
import org.apache.spark.sql.execution.SparkPlan;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

class AdaptivePlanEventFilterTest {

  private static final String ADAPTIVE_SPARK_PLAN = "AdaptiveSparkPlan";

  private final OpenLineageContext context = mock(OpenLineageContext.class);
  private final AdaptivePlanEventFilter filter = new AdaptivePlanEventFilter(context);
  private final SparkListenerEvent sparkListenerEvent = mock(SparkListenerEvent.class);
  private final QueryExecution queryExecution = mock(QueryExecution.class);
  private final SparkPlan sparkPlan = mock(SparkPlan.class);

  @BeforeEach
  public void setup() {
    when(context.getQueryExecution()).thenReturn(Optional.of(queryExecution));
    when(queryExecution.executedPlan()).thenReturn(sparkPlan);
  }

  @Test
  void testAdaptivePlanIsFiltered() {
    try (MockedStatic<EventFilterUtils> mocked = mockStatic(EventFilterUtils.class)) {
      when(EventFilterUtils.isDeltaPlan()).thenReturn(true);
      when(EventFilterUtils.isDeltaWritePlan(context)).thenReturn(true);
      when(sparkPlan.nodeName()).thenReturn(ADAPTIVE_SPARK_PLAN);
      assertTrue(filter.isDisabled(sparkListenerEvent));
    }
  }

  @Test
  void testAdaptiveNonDeltaWritePlanIsNotFiltered() {
    try (MockedStatic<EventFilterUtils> mocked = mockStatic(EventFilterUtils.class)) {
      when(EventFilterUtils.isDeltaPlan()).thenReturn(true);
      when(EventFilterUtils.isDeltaWritePlan(context)).thenReturn(false);
      when(sparkPlan.nodeName()).thenReturn(ADAPTIVE_SPARK_PLAN);
      assertFalse(filter.isDisabled(sparkListenerEvent));
    }
  }

  @Test
  void testWhenQueryExecutionIsNull() {
    try (MockedStatic<EventFilterUtils> mocked = mockStatic(EventFilterUtils.class)) {
      when(EventFilterUtils.isDeltaPlan()).thenReturn(true);
      when(EventFilterUtils.isDeltaWritePlan(context)).thenReturn(true);
      when(context.getQueryExecution()).thenReturn(Optional.ofNullable(null));
      assertFalse(filter.isDisabled(sparkListenerEvent));
    }
  }

  @Test
  void testWhenSparkPlanIsNull() {
    try (MockedStatic<EventFilterUtils> mocked = mockStatic(EventFilterUtils.class)) {
      when(EventFilterUtils.isDeltaPlan()).thenReturn(true);
      when(EventFilterUtils.isDeltaWritePlan(context)).thenReturn(true);
      when(queryExecution.executedPlan()).thenReturn(null);
      assertFalse(filter.isDisabled(sparkListenerEvent));
    }
  }

  @Test
  void testOtherSparkPlan() {
    try (MockedStatic<EventFilterUtils> mocked = mockStatic(EventFilterUtils.class)) {
      when(EventFilterUtils.isDeltaPlan()).thenReturn(true);
      when(EventFilterUtils.isDeltaWritePlan(context)).thenReturn(true);
      when(sparkPlan.nodeName()).thenReturn("OtherSparkPlan");
      assertFalse(filter.isDisabled(sparkListenerEvent));
    }
  }

  @Test
  void testNonDeltaPlan() {
    try (MockedStatic<EventFilterUtils> mocked = mockStatic(EventFilterUtils.class)) {
      when(EventFilterUtils.isDeltaPlan()).thenReturn(false);
      when(sparkPlan.nodeName()).thenReturn(ADAPTIVE_SPARK_PLAN);
      assertFalse(filter.isDisabled(sparkListenerEvent));
    }
  }
}
