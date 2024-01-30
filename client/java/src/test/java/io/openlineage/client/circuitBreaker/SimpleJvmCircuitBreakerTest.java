/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.circuitBreaker;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import io.openlineage.client.circuitBreaker.SimpleJvmCircuitBreaker.CircuitBreakerRuntime;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

public class SimpleJvmCircuitBreakerTest {
  long MEGABYTE = 1024L * 1024L;
  SimpleJvmCircuitBreakerConfig config = mock(SimpleJvmCircuitBreakerConfig.class);
  SimpleJvmCircuitBreaker circuitBreaker = new SimpleJvmCircuitBreaker(config);

  @Test
  void testWhenThresholdIsEmpty() {
    when(config.getMemoryThreshold()).thenReturn(null);
    assertFalse(circuitBreaker.isClosed());
  }

  @Test
  void testWhenThresholdIsReached() {
    when(config.getMemoryThreshold()).thenReturn(20);
    try (MockedStatic mocked = mockStatic(CircuitBreakerRuntime.class)) {
      when(CircuitBreakerRuntime.totalMemory()).thenReturn(26000L * MEGABYTE + MEGABYTE);
      when(CircuitBreakerRuntime.freeMemory()).thenReturn(2000L * MEGABYTE - MEGABYTE);
      when(CircuitBreakerRuntime.maxMemory()).thenReturn(30000L * MEGABYTE);
      assertTrue(circuitBreaker.isClosed());
    }
  }

  @Test
  void testWhenThresholdIsNotReached() {
    when(config.getMemoryThreshold()).thenReturn(20);
    try (MockedStatic mocked = mockStatic(CircuitBreakerRuntime.class)) {
      when(CircuitBreakerRuntime.totalMemory()).thenReturn(26000L * MEGABYTE - MEGABYTE);
      when(CircuitBreakerRuntime.freeMemory()).thenReturn(2000L * MEGABYTE + MEGABYTE);
      when(CircuitBreakerRuntime.maxMemory()).thenReturn(30000L * MEGABYTE);
      assertFalse(circuitBreaker.isClosed());
    }
  }

  @Test
  void testWhenThresholdIsWrong() {
    when(config.getMemoryThreshold()).thenReturn(0);
    assertFalse(circuitBreaker.isClosed());

    when(config.getMemoryThreshold()).thenReturn(101);
    assertFalse(circuitBreaker.isClosed());
  }
}
