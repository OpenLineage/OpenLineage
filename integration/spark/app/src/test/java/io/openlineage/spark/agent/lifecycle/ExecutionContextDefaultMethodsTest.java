/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle;

import static org.assertj.core.api.Assertions.assertThat;

import io.openlineage.client.OpenLineage.RunEvent.EventType;
import org.apache.spark.scheduler.ActiveJob;
import org.apache.spark.scheduler.SparkListenerApplicationEnd;
import org.apache.spark.scheduler.SparkListenerApplicationStart;
import org.apache.spark.scheduler.SparkListenerJobEnd;
import org.apache.spark.scheduler.SparkListenerJobStart;
import org.apache.spark.scheduler.SparkListenerStageCompleted;
import org.apache.spark.scheduler.SparkListenerStageSubmitted;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart;
import org.junit.jupiter.api.Test;

/**
 * Tests for ExecutionContext interface default methods. Uses a minimal mock implementation to test
 * the default status tracking behavior.
 */
class ExecutionContextDefaultMethodsTest {

  /**
   * Minimal mock implementation that only uses default methods from ExecutionContext. This allows
   * us to test the interface's default behavior in isolation.
   */
  private static class MockExecutionContext implements ExecutionContext {
    @Override
    public void setActiveJob(ActiveJob activeJob) {}

    @Override
    public void start(SparkListenerApplicationStart applicationStart) {}

    @Override
    public void start(SparkListenerJobStart jobStart) {}

    @Override
    public void start(SparkListenerSQLExecutionStart sqlStart) {}

    @Override
    public void start(SparkListenerStageSubmitted stageSubmitted) {}

    @Override
    public void end(SparkListenerApplicationEnd applicationEnd) {}

    @Override
    public void end(SparkListenerJobEnd jobEnd) {}

    @Override
    public void end(SparkListenerSQLExecutionEnd sqlEnd) {}

    @Override
    public void end(SparkListenerStageCompleted stageCompleted) {}

    // getStatus() and updateStatus() will use the interface's default implementations
  }

  @Test
  void testDefaultGetStatusReturnsStart() {
    ExecutionContext context = new MockExecutionContext();

    // Default implementation uses shared static status initialized to START
    assertThat(context.getStatus()).isEqualTo(EventType.START);
  }

  @Test
  void testDefaultUpdateToRunningAndComplete() {
    ExecutionContext context = new MockExecutionContext();

    assertThat(context.updateStatus(EventType.RUNNING).getStatus()).isEqualTo(EventType.RUNNING);
    assertThat(context.getStatus()).isEqualTo(EventType.RUNNING);

    assertThat(context.updateStatus(EventType.COMPLETE).getStatus()).isEqualTo(EventType.COMPLETE);
    assertThat(context.getStatus()).isEqualTo(EventType.COMPLETE);
  }

  @Test
  void testDefaultCannotChangeCompleteToFail() {
    ExecutionContext context = new MockExecutionContext();

    assertThat(context.updateStatus(EventType.COMPLETE).getStatus()).isEqualTo(EventType.COMPLETE);
    assertThat(context.getStatus()).isEqualTo(EventType.COMPLETE);

    assertThat(context.updateStatus(EventType.FAIL).getStatus()).isEqualTo(EventType.COMPLETE);
    assertThat(context.getStatus()).isEqualTo(EventType.COMPLETE);
  }

  @Test
  void testDefaultCannotChangeFailToComplete() {
    ExecutionContext context = new MockExecutionContext();

    assertThat(context.updateStatus(EventType.COMPLETE).getStatus()).isEqualTo(EventType.COMPLETE);
    assertThat(context.getStatus()).isEqualTo(EventType.COMPLETE);

    assertThat(context.updateStatus(EventType.FAIL).getStatus()).isEqualTo(EventType.COMPLETE);
    assertThat(context.getStatus()).isEqualTo(EventType.COMPLETE);
  }
}
