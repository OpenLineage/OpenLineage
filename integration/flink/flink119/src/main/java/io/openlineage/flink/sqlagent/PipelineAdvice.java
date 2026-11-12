/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.sqlagent;

import net.bytebuddy.asm.Advice;

/** Associates the active SQL capture with the submitted Flink job. */
public final class PipelineAdvice {
  private PipelineAdvice() {}

  @Advice.OnMethodExit(onThrowable = Throwable.class, suppress = Throwable.class)
  public static void exit(
      @Advice.Argument(0) Object pipeline,
      @Advice.Return Object result,
      @Advice.Thrown Throwable error) {
    if (error == null) AgentBridge.submitted(pipeline, result);
  }
}
