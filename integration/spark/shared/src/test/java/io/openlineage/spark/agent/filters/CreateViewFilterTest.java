/*
/* Copyright 2018-2023 contributors to the OpenLineage project
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
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.plans.logical.Aggregate;
import org.apache.spark.sql.execution.QueryExecution;
import org.apache.spark.sql.execution.command.CreateViewCommand;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

public class CreateViewFilterTest {

  OpenLineageContext context = mock(OpenLineageContext.class);
  CreateViewFilter filter = new CreateViewFilter(context);
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
  void testCreateViewFiltered() {
    try (MockedStatic mocked = mockStatic(SparkSession.class)) {
      when(SparkSession.active()).thenReturn(sparkSession);
      when(context.getSparkVersion()).thenReturn("3.4");
      when(queryExecution.optimizedPlan()).thenReturn(mock(CreateViewCommand.class));

      assertTrue(filter.isDisabled(sparkListenerEvent));
    }
  }

  @Test
  void testCreateViewNotFiltered() {
    try (MockedStatic mocked = mockStatic(SparkSession.class)) {
      when(SparkSession.active()).thenReturn(sparkSession);
      when(context.getSparkVersion()).thenReturn("3.2");
      when(queryExecution.optimizedPlan()).thenReturn(mock(CreateViewCommand.class));

      assertFalse(filter.isDisabled(sparkListenerEvent));
    }
  }

  @Test
  void testOtherNotFiltered() {
    try (MockedStatic mocked = mockStatic(SparkSession.class)) {
      when(SparkSession.active()).thenReturn(sparkSession);
      when(context.getSparkVersion()).thenReturn("3.2");
      when(queryExecution.optimizedPlan()).thenReturn(mock(Aggregate.class));

      assertFalse(filter.isDisabled(sparkListenerEvent));
    }
  }
}
