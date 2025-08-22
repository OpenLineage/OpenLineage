/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.circuitBreaker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.openlineage.client.metrics.MicrometerProvider;
import java.util.concurrent.Callable;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

class TaskQueueCircuitBreakerTest {
  TaskQueueCircuitBreaker circuitBreaker =
      new TaskQueueCircuitBreaker(new TaskQueueCircuitBreakerConfig(1, 1, 1L, 1L, 200));

  MeterRegistry meterRegistry = new SimpleMeterRegistry();

  Callable<Object> infiniteCallable =
      () -> {
        while (true) {
          Thread.sleep(1000);
        }
      };

  @Test
  void testTaskQueueBasedExecution() {
    try (MockedStatic<MicrometerProvider> mocked = mockStatic(MicrometerProvider.class)) {
      when(MicrometerProvider.getMeterRegistry()).thenReturn(meterRegistry);

      assertThat(circuitBreaker.<Object>run(infiniteCallable)).isNull();
      assertThat(circuitBreaker.<Object>run(infiniteCallable)).isNull();
      assertThat(circuitBreaker.<Object>run(infiniteCallable)).isNull();

      assertThat(meterRegistry.counter(TaskQueueCircuitBreaker.TIMED_OUT_METRIC).count())
          .isEqualTo(2D);
      assertThat(meterRegistry.counter(TaskQueueCircuitBreaker.DROPPED_METRIC).count())
          .isEqualTo(1D);
      assertThat(circuitBreaker.getPendingTasks()).isEqualTo(1L);
      circuitBreaker.close();
    }
  }

  @Test
  void testMetricRegistryIsNull() {
    try (MockedStatic<MicrometerProvider> mocked = mockStatic(MicrometerProvider.class)) {
      when(MicrometerProvider.getMeterRegistry()).thenReturn(null);

      assertThat(circuitBreaker.<Object>run(infiniteCallable)).isNull();
      assertThat(circuitBreaker.<Object>run(infiniteCallable)).isNull();
      assertThat(circuitBreaker.<Object>run(infiniteCallable)).isNull();

      assertThat(circuitBreaker.getPendingTasks()).isEqualTo(1L);
      circuitBreaker.close();
    }
  }
}
