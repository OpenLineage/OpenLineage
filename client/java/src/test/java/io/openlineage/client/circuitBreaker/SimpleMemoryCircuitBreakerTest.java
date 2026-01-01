/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.circuitBreaker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import io.openlineage.client.utils.RuntimeUtils;
import java.lang.management.GarbageCollectorMXBean;
import java.util.Collections;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

class SimpleMemoryCircuitBreakerTest {
  long MEGABYTE = 1024L * 1024L;
  SimpleMemoryCircuitBreakerConfig config = mock(SimpleMemoryCircuitBreakerConfig.class);
  SimpleMemoryCircuitBreaker circuitBreaker = new SimpleMemoryCircuitBreaker(config);
  GarbageCollectorMXBean gcBean = mock(GarbageCollectorMXBean.class);

  @Test
  void testWhenThresholdIsEmpty() {
    when(config.getMemoryThreshold()).thenReturn(null);
    assertFalse(circuitBreaker.currentState().isClosed());
  }

  @Test
  void testWhenThresholdIsReached() {
    when(config.getMemoryThreshold()).thenReturn(20);
    try (MockedStatic mocked = mockStatic(RuntimeUtils.class)) {
      when(RuntimeUtils.totalMemory()).thenReturn(26000L * MEGABYTE + MEGABYTE);
      when(RuntimeUtils.freeMemory()).thenReturn(2000L * MEGABYTE - MEGABYTE);
      when(RuntimeUtils.maxMemory()).thenReturn(30000L * MEGABYTE);
      when(RuntimeUtils.getGarbageCollectorMXBeans()).thenReturn(Collections.singletonList(gcBean));
      when(gcBean.getCollectionTime()).thenReturn(1000l);
      when(gcBean.getCollectionCount()).thenReturn(1l);
      assertTrue(circuitBreaker.currentState().isClosed());
      assertThat(circuitBreaker.currentState().getReason().get())
          .isEqualTo("Circuit breaker tripped at memory 19.99% (freeMemoryThreshold 20%)");
    }
  }

  @Test
  void testWhenThresholdIsNotReached() {
    when(config.getMemoryThreshold()).thenReturn(20);
    try (MockedStatic mocked = mockStatic(RuntimeUtils.class)) {
      when(RuntimeUtils.totalMemory()).thenReturn(26000L * MEGABYTE - MEGABYTE);
      when(RuntimeUtils.freeMemory()).thenReturn(2000L * MEGABYTE + MEGABYTE);
      when(RuntimeUtils.maxMemory()).thenReturn(30000L * MEGABYTE);
      assertFalse(circuitBreaker.currentState().isClosed());
    }
  }

  @Test
  void testWhenThresholdIsWrong() {
    when(config.getMemoryThreshold()).thenReturn(0);
    assertFalse(circuitBreaker.currentState().isClosed());

    when(config.getMemoryThreshold()).thenReturn(101);
    assertFalse(circuitBreaker.currentState().isClosed());
  }

  @Test
  void testCheckInterval() {
    when(config.getCircuitCheckIntervalInMillis()).thenReturn(200);
    circuitBreaker = new SimpleMemoryCircuitBreaker(config);
    assertThat(circuitBreaker.getCheckIntervalMillis()).isEqualTo(200);
  }
}
