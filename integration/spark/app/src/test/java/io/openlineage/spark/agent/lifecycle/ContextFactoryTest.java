/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.openlineage.spark.agent.EventEmitter;
import io.openlineage.spark.api.SparkOpenLineageConfig;
import java.util.Collections;
import java.util.Optional;
import java.util.UUID;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.junit.jupiter.api.Test;

class ContextFactoryTest {

  @Test
  void testApplicationJobNameIsResolvedDuringContextCreation() {
    EventEmitter emitter = mock(EventEmitter.class);
    when(emitter.getApplicationRunId()).thenReturn(UUID.randomUUID());
    when(emitter.getCustomEnvironmentVariables()).thenReturn(Optional.of(Collections.emptyList()));

    SparkContext sparkContext = mock(SparkContext.class);
    SparkConf sparkConf = new SparkConf().set("spark.app.name", "test-app");
    when(sparkContext.getConf()).thenReturn(sparkConf);
    when(sparkContext.appName()).thenReturn("test-app");

    SparkOpenLineageConfig config = new SparkOpenLineageConfig();

    ContextFactory factory = new ContextFactory(emitter, new SimpleMeterRegistry(), config);
    factory.createSparkApplicationExecutionContext(sparkContext);

    verify(emitter).setApplicationJobName("test_app");
  }
}
