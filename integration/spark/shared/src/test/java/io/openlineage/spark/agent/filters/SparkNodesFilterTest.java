/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.filters;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import io.openlineage.spark.api.OpenLineageContext;
import java.util.Arrays;
import java.util.Optional;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.plans.logical.Aggregate;
import org.apache.spark.sql.execution.QueryExecution;
import org.apache.spark.sql.execution.datasources.CreateTable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

class SparkNodesFilterTest {

  OpenLineageContext context = mock(OpenLineageContext.class, RETURNS_DEEP_STUBS);
  SparkNodesFilter filter = new SparkNodesFilter(context);
  SparkSession sparkSession = mock(SparkSession.class);
  SparkContext sparkContext = mock(SparkContext.class);
  SparkConf sparkConf = mock(SparkConf.class);
  SparkListenerEvent sparkListenerEvent = mock(SparkListenerEvent.class);
  QueryExecution queryExecution = mock(QueryExecution.class);

  @BeforeEach
  public void setup() {
    when(sparkSession.sparkContext()).thenReturn(sparkContext);
    when(sparkContext.conf()).thenReturn(sparkConf);
    when(context.getQueryExecution()).thenReturn(Optional.of(queryExecution));
  }

  @Test
  void testSparkPlanToBeFiltered() {
    try (MockedStatic mocked = mockStatic(SparkSession.class)) {
      when(SparkSession.active()).thenReturn(sparkSession);
      when(queryExecution.optimizedPlan()).thenReturn(mock(Aggregate.class));

      assertTrue(filter.isDisabled(sparkListenerEvent));
    }
  }

  @Test
  void testSparkPlanThatShouldNotBeFiltered() {
    try (MockedStatic mocked = mockStatic(SparkSession.class)) {
      when(SparkSession.active()).thenReturn(sparkSession);
      when(queryExecution.optimizedPlan()).thenReturn(mock(CreateTable.class));

      assertFalse(filter.isDisabled(sparkListenerEvent));
    }
  }

  @Test
  void testNodeAllowedInConfig() {
    when(context.getOpenLineageConfig().getFilterConfig().getAllowedSparkNodes())
        .thenReturn(Arrays.asList("org.apache.spark.sql.catalyst.plans.logical.Aggregate"));

    try (MockedStatic mocked = mockStatic(SparkSession.class)) {
      when(SparkSession.active()).thenReturn(sparkSession);
      when(queryExecution.optimizedPlan()).thenReturn(mock(Aggregate.class));

      assertFalse(filter.isDisabled(sparkListenerEvent));
    }
  }

  @Test
  void testNodeDeniedInConfig() {
    when(context.getOpenLineageConfig().getFilterConfig().getDeniedSparkNodes())
        .thenReturn(Arrays.asList("org.apache.spark.sql.execution.datasources.CreateTable"));

    try (MockedStatic mocked = mockStatic(SparkSession.class)) {
      when(SparkSession.active()).thenReturn(sparkSession);
      when(queryExecution.optimizedPlan()).thenReturn(mock(CreateTable.class));

      assertTrue(filter.isDisabled(sparkListenerEvent));
    }
  }
}
