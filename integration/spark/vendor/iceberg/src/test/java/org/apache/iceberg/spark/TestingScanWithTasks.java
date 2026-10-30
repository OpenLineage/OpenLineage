/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package org.apache.iceberg.spark;

import java.util.List;
import org.apache.iceberg.ScanTask;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.types.StructType;

public class TestingScanWithTasks implements Scan {
  private final List<ScanTask> tasks;
  private int tasksCalls;

  public TestingScanWithTasks(List<ScanTask> tasks) {
    this.tasks = tasks;
  }

  public List<ScanTask> tasks() {
    tasksCalls++;
    return tasks;
  }

  public int getTasksCalls() {
    return tasksCalls;
  }

  @Override
  public StructType readSchema() {
    return new StructType();
  }
}
