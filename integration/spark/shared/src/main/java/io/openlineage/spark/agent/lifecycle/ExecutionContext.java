/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle;

import static io.openlineage.client.OpenLineage.RunEvent.EventType.START;

import io.openlineage.client.OpenLineage.RunEvent.EventType;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.spark.scheduler.ActiveJob;
import org.apache.spark.scheduler.SparkListenerApplicationEnd;
import org.apache.spark.scheduler.SparkListenerApplicationStart;
import org.apache.spark.scheduler.SparkListenerJobEnd;
import org.apache.spark.scheduler.SparkListenerJobStart;
import org.apache.spark.scheduler.SparkListenerStageCompleted;
import org.apache.spark.scheduler.SparkListenerStageSubmitted;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart;

public interface ExecutionContext {

  /**
   * Pattern used to match a variety of patterns in job names, including camel case and token
   * separated (whitespace, dash, or underscore), with special handling for upper-case
   * abbreviations, like XML or JDBC
   */
  String CAMEL_TO_SNAKE_CASE =
      "[\\s\\-_]?((?<=.)[A-Z](?=[a-z\\s\\-_])|(?<=[^A-Z])[A-Z]|((?<=[\\s\\-_])[a-z\\d]))";

  AtomicReference<EventType> status = new AtomicReference<>(START);

  void setActiveJob(ActiveJob activeJob);

  void start(SparkListenerApplicationStart applicationStart);

  void start(SparkListenerJobStart jobStart);

  void start(SparkListenerSQLExecutionStart sqlStart);

  void start(SparkListenerStageSubmitted stageSubmitted);

  void end(SparkListenerApplicationEnd applicationEnd);

  void end(SparkListenerJobEnd jobEnd);

  void end(SparkListenerSQLExecutionEnd sqlEnd);

  void end(SparkListenerStageCompleted stageCompleted);

  default Optional<Integer> getActiveJobId() {
    return Optional.empty();
  }

  default void setActiveJobId(Integer activeJobId) {}

  default EventType getStatus() {
    return status.get();
  }

  default ExecutionContext updateStatus(EventType newStatus) {
    status.updateAndGet(c -> newStatus);
    return this;
  }
}
