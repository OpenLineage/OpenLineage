/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.when;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.openlineage.client.OpenLineage.RunEvent;
import io.openlineage.spark.agent.lifecycle.StaticExecutionContextFactory;
import io.openlineage.spark.api.SparkOpenLineageConfig;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.commons.beanutils.BeanUtils;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** JUnit extension that provides mock EventEmitter as test parameter. */
public class EventEmitterProviderExtension implements AfterAllCallback, ParameterResolver {

  public static final Logger logger = LoggerFactory.getLogger(EventEmitterProviderExtension.class);

  public static void setup(EventEmitter emitter) {
    prepareEventEmitter(emitter);

    OpenLineageSparkListener.init(
        new StaticExecutionContextFactory(
            emitter, new SimpleMeterRegistry(), new SparkOpenLineageConfig()));
  }

  public static void stop() {
    OpenLineageSparkListener.close();
  }

  @Override
  public boolean supportsParameter(
      ParameterContext parameterContext, ExtensionContext extensionContext)
      throws ParameterResolutionException {
    return parameterContext.getParameter().getType().equals(EventEmitter.class);
  }

  @Override
  public Object resolveParameter(
      ParameterContext parameterContext, ExtensionContext extensionContext)
      throws ParameterResolutionException {
    EventEmitter emitter = mock(EventEmitter.class);
    setup(emitter);
    return emitter;
  }

  @Override
  public void afterAll(ExtensionContext context) throws Exception {
    stop();
  }

  private static void prepareEventEmitter(EventEmitter emitter) {
    reset(emitter);
    when(emitter.getJobNamespace()).thenReturn("ns_name");
    when(emitter.getParentJobName()).thenReturn(Optional.of("parent_name"));
    when(emitter.getParentJobNamespace()).thenReturn(Optional.of("parent_namespace"));
    when(emitter.getParentRunId())
        .thenReturn(Optional.of(UUID.fromString("8d99e33e-2a1c-4254-9600-18f23435fc3b")));
    when(emitter.getApplicationRunId())
        .thenReturn(UUID.fromString("8d99e33e-bbbb-cccc-dddd-18f2343aaaaa"));
    when(emitter.getApplicationJobName()).thenReturn("test_rdd");
    doAnswer(
            (input) -> {
              logger.info(
                  "Emit called with args {}",
                  Arrays.stream(input.getArguments())
                      .map(arg -> describe(arg))
                      .collect(Collectors.toList()));
              return null;
            })
        .when(emitter)
        .emit(any(RunEvent.class));
  }

  private static Map describe(Object arg) {
    try {
      return BeanUtils.describe(arg);
    } catch (Exception e) {
      logger.error("Unable to describe event {}", arg, e);
      return new HashMap();
    }
  }
}
