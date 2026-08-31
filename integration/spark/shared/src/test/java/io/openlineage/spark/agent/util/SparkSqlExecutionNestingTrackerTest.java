/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.util;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import java.util.OptionalLong;
import org.apache.spark.sql.catalyst.plans.logical.Command;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.QueryExecution;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart;
import org.junit.jupiter.api.Test;

class SparkSqlExecutionNestingTrackerTest {

  private final SparkSqlExecutionNestingTracker tracker = new SparkSqlExecutionNestingTracker();

  @Test
  void testUsesRootExecutionIdWhenSparkProvidesIt() {
    SparkListenerSQLExecutionStart root = event(1, "root", "user call site");
    SparkListenerSQLExecutionStart child = event(2, "child", "different call site");

    assertFalse(tracker.register(root, commandExecution(), OptionalLong.of(1)));
    assertTrue(tracker.register(child, queryExecution(), OptionalLong.of(1)));
    assertTrue(tracker.isNested(2));
    assertTrue(tracker.isCommandChild(2));
    tracker.end(2);
    assertFalse(tracker.isNested(2));
    assertFalse(tracker.isCommandChild(2));
  }

  @Test
  void testNestedExecutionUnderNonCommandRootIsNotCommandChild() {
    SparkListenerSQLExecutionStart root = event(1, "root", "user call site");
    SparkListenerSQLExecutionStart child = event(2, "child", "different call site");

    assertFalse(tracker.register(root, queryExecution(), OptionalLong.of(1)));
    assertTrue(tracker.register(child, queryExecution(), OptionalLong.of(1)));
    assertFalse(tracker.isCommandChild(2));
  }

  @Test
  void testUnknownRootFailsOpen() {
    assertTrue(
        tracker.register(
            event(2, "child", "org.apache.spark.sql.delta.internal"),
            queryExecution(),
            OptionalLong.of(1)));
    assertFalse(tracker.isCommandChild(2));
  }

  @Test
  void testLegacyChildWithSameCallSiteIsNested() {
    SparkListenerSQLExecutionStart root = event(1, "save", "shared call site");
    SparkListenerSQLExecutionStart child = event(2, "save", "shared call site");

    assertFalse(tracker.register(root, commandExecution(), OptionalLong.empty()));
    assertTrue(tracker.register(child, queryExecution(), OptionalLong.empty()));
    assertTrue(tracker.isCommandChild(2));
  }

  @Test
  void testLegacyDeltaChildIsNested() {
    QueryExecution rootExecution = commandExecution();

    assertFalse(
        tracker.register(event(1, "save", "user call site"), rootExecution, OptionalLong.empty()));

    assertTrue(
        tracker.register(
            event(2, "Delta operation", "org.apache.spark.sql.delta.commands.WriteIntoDelta"),
            queryExecution(),
            OptionalLong.empty()));
    assertTrue(tracker.isCommandChild(2));
    tracker.end(2);

    assertTrue(
        tracker.register(
            event(
                3,
                "Databricks Delta operation",
                "com.databricks.sql.transaction.tahoe.commands.WriteIntoDeltaCommand"),
            queryExecution(),
            OptionalLong.empty()));
    assertTrue(tracker.isCommandChild(3));
  }

  @Test
  void testLegacyConcurrentTopLevelQueryFailsOpen() {
    assertFalse(
        tracker.register(
            event(1, "save", "writer call site"), commandExecution(), OptionalLong.empty()));

    assertFalse(
        tracker.register(
            event(2, "collect", "reader call site"), queryExecution(), OptionalLong.empty()));
  }

  @Test
  void testLegacyConcurrentCommandFailsOpenEvenAtSameCallSite() {
    assertFalse(
        tracker.register(
            event(1, "save", "shared call site"), commandExecution(), OptionalLong.empty()));

    assertFalse(
        tracker.register(
            event(2, "save", "shared call site"), commandExecution(), OptionalLong.empty()));
  }

  @Test
  void testLegacyAmbiguousCommandParentsFailOpen() {
    tracker.register(event(1, "save", "first"), commandExecution(), OptionalLong.empty());
    tracker.register(event(2, "save", "second"), commandExecution(), OptionalLong.empty());

    assertFalse(
        tracker.register(
            event(3, "Delta operation", "org.apache.spark.sql.delta.commands.WriteIntoDelta"),
            queryExecution(),
            OptionalLong.empty()));
  }

  @Test
  void testCompletedRootIsNotUsedForLegacyCorrelation() {
    tracker.register(
        event(1, "save", "shared call site"), commandExecution(), OptionalLong.empty());
    tracker.end(1);

    assertFalse(
        tracker.register(
            event(2, "save", "shared call site"), queryExecution(), OptionalLong.empty()));
  }

  private static SparkListenerSQLExecutionStart event(
      long executionId, String description, String details) {
    SparkListenerSQLExecutionStart event = mock(SparkListenerSQLExecutionStart.class);
    when(event.executionId()).thenReturn(executionId);
    when(event.description()).thenReturn(description);
    when(event.details()).thenReturn(details);
    return event;
  }

  private static QueryExecution commandExecution() {
    LogicalPlan command = mock(LogicalPlan.class, withSettings().extraInterfaces(Command.class));
    QueryExecution queryExecution = mock(QueryExecution.class);
    when(queryExecution.optimizedPlan()).thenReturn(command);
    return queryExecution;
  }

  private static QueryExecution queryExecution() {
    QueryExecution queryExecution = mock(QueryExecution.class);
    when(queryExecution.optimizedPlan()).thenReturn(mock(LogicalPlan.class));
    return queryExecution;
  }
}
