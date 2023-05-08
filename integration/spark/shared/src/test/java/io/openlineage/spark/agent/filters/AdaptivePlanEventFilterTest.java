/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.filters;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.openlineage.spark.api.OpenLineageContext;
import java.util.Optional;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.execution.QueryExecution;
import org.apache.spark.sql.execution.SparkPlan;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class AdaptivePlanEventFilterTest {

  OpenLineageContext context = mock(OpenLineageContext.class);
  AdaptivePlanEventFilter filter = new AdaptivePlanEventFilter(context);
  SparkListenerEvent sparkListenerEvent = mock(SparkListenerEvent.class);
  QueryExecution queryExecution = mock(QueryExecution.class);

  SparkPlan sparkPlan = mock(SparkPlan.class);

  @BeforeEach
  public void setup() {
    when(context.getQueryExecution()).thenReturn(Optional.of(queryExecution));
    when(queryExecution.executedPlan()).thenReturn(sparkPlan);
  }

  @Test
  void testAdaptivePlanIsFiltered() {
    when(sparkPlan.nodeName()).thenReturn("AdaptiveSparkPlan");
    assertTrue(filter.isDisabled(sparkListenerEvent));
  }

  @Test
  void testWhenQueryExecutionIsNull() {
    when(context.getQueryExecution()).thenReturn(Optional.ofNullable(null));
    assertFalse(filter.isDisabled(sparkListenerEvent));
  }

  @Test
  void testWhenSparkPlanIsNull() {
    when(queryExecution.executedPlan()).thenReturn(null);
    assertFalse(filter.isDisabled(sparkListenerEvent));
  }

  @Test
  void testOtherSparkPlan() {
    when(sparkPlan.nodeName()).thenReturn("OtherSparkPlan");
    assertFalse(filter.isDisabled(sparkListenerEvent));
  }
}
