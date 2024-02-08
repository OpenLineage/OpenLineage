/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.circuitBreaker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import java.lang.management.GarbageCollectorMXBean;
import java.util.Collections;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

public class SimpleMemoryCircuitBreakerTest {
  long MEGABYTE = 1024L * 1024L;
  SimpleMemoryCircuitBreakerConfig config = mock(SimpleMemoryCircuitBreakerConfig.class);
  SimpleMemoryCircuitBreaker circuitBreaker = new SimpleMemoryCircuitBreaker(config);
  GarbageCollectorMXBean gcBean = mock(GarbageCollectorMXBean.class);

  @Test
  void testWhenThresholdIsEmpty() {
    when(config.getMemoryThreshold()).thenReturn(null);
    assertFalse(circuitBreaker.isClosed());
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
      assertTrue(circuitBreaker.isClosed());
    }
  }

  @Test
  void testWhenThresholdIsNotReached() {
    when(config.getMemoryThreshold()).thenReturn(20);
    try (MockedStatic mocked = mockStatic(RuntimeUtils.class)) {
      when(RuntimeUtils.totalMemory()).thenReturn(26000L * MEGABYTE - MEGABYTE);
      when(RuntimeUtils.freeMemory()).thenReturn(2000L * MEGABYTE + MEGABYTE);
      when(RuntimeUtils.maxMemory()).thenReturn(30000L * MEGABYTE);
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

  @Test
  void testCheckInterval() {
    when(config.getCircuitCheckIntervalInMillis()).thenReturn(200);
    circuitBreaker = new SimpleMemoryCircuitBreaker(config);
    assertThat(circuitBreaker.getCheckIntervalMillis()).isEqualTo(200);
  }
}
