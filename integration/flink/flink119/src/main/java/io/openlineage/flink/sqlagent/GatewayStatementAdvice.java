/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.sqlagent;

import net.bytebuddy.asm.Advice;

/** Keeps the SQL Gateway statement available while Flink submits its job. */
public final class GatewayStatementAdvice {
  private GatewayStatementAdvice() {}

  @Advice.OnMethodEnter(suppress = Throwable.class)
  public static String enter(@Advice.Argument(1) String statement) {
    return AgentBridge.beginStatement(statement);
  }

  @Advice.OnMethodExit(onThrowable = Throwable.class, suppress = Throwable.class)
  public static void exit(@Advice.Enter String previous) {
    AgentBridge.endStatement(previous);
  }
}
