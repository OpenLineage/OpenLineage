/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.sqlagent;

import net.bytebuddy.asm.Advice;

/** Captures the resolved SQL plan before Flink translates it to a StreamGraph. */
public final class TableOperationAdvice {
  private TableOperationAdvice() {}

  @Advice.OnMethodEnter(suppress = Throwable.class)
  public static Object enter(
      @Advice.This Object environment, @Advice.Argument(0) Object operations) {
    return AgentBridge.beginSql(environment, operations);
  }

  @Advice.OnMethodExit(onThrowable = Throwable.class, suppress = Throwable.class)
  public static void exit(@Advice.Enter Object previous) {
    AgentBridge.endSql(previous);
  }
}
