/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.sqlagent;

import static net.bytebuddy.matcher.ElementMatchers.hasSuperType;
import static net.bytebuddy.matcher.ElementMatchers.isInterface;
import static net.bytebuddy.matcher.ElementMatchers.named;
import static net.bytebuddy.matcher.ElementMatchers.not;
import static net.bytebuddy.matcher.ElementMatchers.takesArgument;
import static net.bytebuddy.matcher.ElementMatchers.takesArguments;

import java.lang.instrument.Instrumentation;
import net.bytebuddy.agent.builder.AgentBuilder;
import net.bytebuddy.asm.Advice;

/** Optional Flink 1 SQL agent. DataStream jobs use the separate OpenLineage Flink listener JAR. */
public final class LineageAgent {
  private LineageAgent() {}

  public static void premain(String arguments, Instrumentation instrumentation) {
    install(instrumentation);
  }

  public static void agentmain(String arguments, Instrumentation instrumentation) {
    install(instrumentation);
  }

  private static void install(Instrumentation instrumentation) {
    AgentBridge.configure();
    new AgentBuilder.Default()
        .with(AgentBuilder.RedefinitionStrategy.RETRANSFORMATION)
        .type(
            hasSuperType(named("org.apache.flink.core.execution.PipelineExecutor"))
                .and(not(isInterface())))
        .transform(
            (builder, type, loader, module, protectionDomain) ->
                builder.visit(
                    Advice.to(PipelineAdvice.class)
                        .on(
                            named("execute")
                                .and(takesArguments(3))
                                .and(
                                    takesArgument(0, named("org.apache.flink.api.dag.Pipeline"))))))
        .type(named("org.apache.flink.table.api.internal.TableEnvironmentImpl"))
        .transform(
            (builder, type, loader, module, protectionDomain) ->
                builder.visit(
                    Advice.to(TableOperationAdvice.class)
                        .on(
                            named("executeInternal")
                                .and(takesArguments(1))
                                .and(takesArgument(0, named("java.util.List"))))))
        .type(named("org.apache.flink.table.gateway.service.operation.OperationExecutor"))
        .transform(
            (builder, type, loader, module, protectionDomain) ->
                builder.visit(
                    Advice.to(GatewayStatementAdvice.class)
                        .on(
                            named("executeStatement")
                                .and(takesArguments(2))
                                .and(takesArgument(1, named("java.lang.String"))))))
        .installOn(instrumentation);
  }
}
