/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle;

import org.apache.spark.scheduler.ActiveJob;
import org.apache.spark.scheduler.SparkListenerJobEnd;
import org.apache.spark.scheduler.SparkListenerJobStart;
import org.apache.spark.scheduler.SparkListenerStageCompleted;
import org.apache.spark.scheduler.SparkListenerStageSubmitted;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart;

public interface ExecutionContext {

  void setActiveJob(ActiveJob activeJob);

  void start(SparkListenerJobStart jobStart);

  void start(SparkListenerSQLExecutionStart sqlStart);

  void start(SparkListenerStageSubmitted stageSubmitted);

  void end(SparkListenerJobEnd jobEnd);

  void end(SparkListenerSQLExecutionEnd sqlEnd);

  void end(SparkListenerStageCompleted stageCompleted);
}
