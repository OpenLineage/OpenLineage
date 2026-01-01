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
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

class JavaRuntimeCircuitBreakerTest {
  long MEGABYTE = 1024L * 1024L;
  JavaRuntimeCircuitBreakerConfig config = mock(JavaRuntimeCircuitBreakerConfig.class);
  JavaRuntimeCircuitBreaker circuitBreaker;
  GarbageCollectorMXBean oldGcBean = mock(GarbageCollectorMXBean.class);
  GarbageCollectorMXBean youngerGcBean = mock(GarbageCollectorMXBean.class);

  @BeforeEach
  void setup() {
    when(config.getMemoryThreshold()).thenReturn(20);
    when(config.getGcCpuThreshold()).thenReturn(10);

    when(oldGcBean.getName()).thenReturn("olgGcBean");
    when(oldGcBean.getCollectionTime())
        .thenReturn(
            TimeUnit.MILLISECONDS.convert(1, TimeUnit.SECONDS),
            TimeUnit.MILLISECONDS.convert(2, TimeUnit.SECONDS),
            TimeUnit.MILLISECONDS.convert(3, TimeUnit.SECONDS),
            TimeUnit.MILLISECONDS.convert(4, TimeUnit.SECONDS));
    when(oldGcBean.getCollectionCount()).thenReturn(2l);

    circuitBreaker = new JavaRuntimeCircuitBreaker(config);

    when(youngerGcBean.getName()).thenReturn("youngerGcBean");
    when(youngerGcBean.getCollectionTime()).thenReturn(0l);
    when(youngerGcBean.getCollectionCount()).thenReturn(3l);
  }

  @Test
  void testWhenThresholdIsEmpty() {
    when(config.getMemoryThreshold()).thenReturn(null);
    assertFalse(circuitBreaker.currentState().isClosed());

    when(config.getMemoryThreshold()).thenReturn(20);
    when(config.getGcCpuThreshold()).thenReturn(null);
    assertFalse(circuitBreaker.currentState().isClosed());
  }

  @Test
  void testWhenThresholdIsReached() {
    try (MockedStatic mocked = mockStatic(RuntimeUtils.class)) {
      when(RuntimeUtils.totalMemory()).thenReturn(26000L * MEGABYTE + MEGABYTE);
      when(RuntimeUtils.freeMemory()).thenReturn(2000L * MEGABYTE - MEGABYTE);
      when(RuntimeUtils.maxMemory()).thenReturn(30000L * MEGABYTE);
      when(RuntimeUtils.getGarbageCollectorMXBeans())
          .thenReturn(Collections.singletonList(oldGcBean));

      // first run to collect elapsed time
      circuitBreaker.currentState().isClosed();

      assertTrue(circuitBreaker.currentState().isClosed());
      assertThat(circuitBreaker.currentState().getReason().get())
          .startsWith("Circuit breaker tripped at memory 19.99%  GC CPU time ")
          .endsWith("(freeMemoryThreshold 20%, gcCPUThreshold 10%)");
    }
  }

  @Test
  void testWhenThresholdIsNotReachedForMemory() {
    when(config.getMemoryThreshold()).thenReturn(20);
    try (MockedStatic mocked = mockStatic(RuntimeUtils.class)) {
      when(RuntimeUtils.totalMemory()).thenReturn(26000L * MEGABYTE - MEGABYTE);
      when(RuntimeUtils.freeMemory()).thenReturn(2000L * MEGABYTE + MEGABYTE);
      when(RuntimeUtils.maxMemory()).thenReturn(30000L * MEGABYTE);
      when(RuntimeUtils.getGarbageCollectorMXBeans())
          .thenReturn(Collections.singletonList(oldGcBean));
      // first run to collect elapsed time
      circuitBreaker.currentState().isClosed();

      assertFalse(circuitBreaker.currentState().isClosed());
    }
  }

  @Test
  void testWhenThresholdIsNotReachedForGcCpuTime() {
    try (MockedStatic mocked = mockStatic(RuntimeUtils.class)) {
      when(RuntimeUtils.totalMemory()).thenReturn(26000L * MEGABYTE + MEGABYTE);
      when(RuntimeUtils.freeMemory()).thenReturn(2000L * MEGABYTE - MEGABYTE);
      when(RuntimeUtils.maxMemory()).thenReturn(30000L * MEGABYTE);

      GarbageCollectorMXBean oldGcBean = mock(GarbageCollectorMXBean.class);
      when(RuntimeUtils.getGarbageCollectorMXBeans())
          .thenReturn(Collections.singletonList(oldGcBean));
      // first run to collect elapsed time
      circuitBreaker.currentState().isClosed();

      when(oldGcBean.getCollectionTime()).thenReturn(0l);
      when(oldGcBean.getCollectionCount()).thenReturn(1l);
      when(oldGcBean.getName()).thenReturn("gcName");

      assertFalse(circuitBreaker.currentState().isClosed());
    }
  }

  @Test
  void testWhenThresholdIsWrong() {
    try (MockedStatic mocked = mockStatic(RuntimeUtils.class)) {
      // make sure circuit breaker is closed
      when(config.getMemoryThreshold()).thenReturn(20);
      when(RuntimeUtils.totalMemory()).thenReturn(26000L * MEGABYTE + MEGABYTE);
      when(RuntimeUtils.freeMemory()).thenReturn(2000L * MEGABYTE - MEGABYTE);
      when(RuntimeUtils.maxMemory()).thenReturn(30000L * MEGABYTE);
      when(RuntimeUtils.getGarbageCollectorMXBeans())
          .thenReturn(Collections.singletonList(oldGcBean));
      // first run to collect elapsed time
      circuitBreaker.currentState().isClosed();

      assertTrue(circuitBreaker.currentState().isClosed());

      // assure it gets open
      when(config.getMemoryThreshold()).thenReturn(-1);
      assertFalse(circuitBreaker.currentState().isClosed());

      when(config.getMemoryThreshold()).thenReturn(101);
      assertFalse(circuitBreaker.currentState().isClosed());

      when(config.getMemoryThreshold()).thenReturn(20);
      assertTrue(circuitBreaker.currentState().isClosed()); // should be closed again

      when(config.getGcCpuThreshold()).thenReturn(101);
      assertFalse(circuitBreaker.currentState().isClosed());
      when(config.getGcCpuThreshold()).thenReturn(-1);
      assertFalse(circuitBreaker.currentState().isClosed());
    }
  }

  @Test
  void testCheckInterval() {
    when(config.getCircuitCheckIntervalInMillis()).thenReturn(200);
    circuitBreaker = new JavaRuntimeCircuitBreaker(config);
    assertThat(circuitBreaker.getCheckIntervalMillis()).isEqualTo(200);
  }

  @Test
  void testGcThresholdMetForOlderGcOnly() {
    try (MockedStatic mocked = mockStatic(RuntimeUtils.class)) {
      when(RuntimeUtils.totalMemory()).thenReturn(26000L * MEGABYTE + MEGABYTE);
      when(RuntimeUtils.freeMemory()).thenReturn(2000L * MEGABYTE - MEGABYTE);
      when(RuntimeUtils.maxMemory()).thenReturn(30000L * MEGABYTE);

      // would be open if only youngerGcBeanPresent
      when(RuntimeUtils.getGarbageCollectorMXBeans())
          .thenReturn(Collections.singletonList(youngerGcBean));
      // first run to collect elapsed time
      circuitBreaker.currentState().isClosed();
      assertFalse(circuitBreaker.currentState().isClosed());

      // should be closed if both gc beans present
      when(RuntimeUtils.getGarbageCollectorMXBeans())
          .thenReturn(Arrays.asList(oldGcBean, youngerGcBean));

      // create new circuit breaker to reload gc beans
      JavaRuntimeCircuitBreaker circuitBreaker = new JavaRuntimeCircuitBreaker(config);
      circuitBreaker.currentState().isClosed();

      assertTrue(circuitBreaker.currentState().isClosed());
    }
  }

  @Test
  void testGcThresholdMetForGcTie() {
    try (MockedStatic mocked = mockStatic(RuntimeUtils.class)) {
      when(RuntimeUtils.totalMemory()).thenReturn(26000L * MEGABYTE + MEGABYTE);
      when(RuntimeUtils.freeMemory()).thenReturn(2000L * MEGABYTE - MEGABYTE);
      when(RuntimeUtils.maxMemory()).thenReturn(30000L * MEGABYTE);

      // would be open if only youngerGcBeanPresent
      when(RuntimeUtils.getGarbageCollectorMXBeans())
          .thenReturn(Arrays.asList(youngerGcBean, oldGcBean));

      // first run to collect elapsed time
      circuitBreaker.currentState().isClosed();

      // should be open when tie with new circuit breaker to reload gc beans
      JavaRuntimeCircuitBreaker circuitBreaker = new JavaRuntimeCircuitBreaker(config);
      when(youngerGcBean.getCollectionCount()).thenReturn(2l);
      assertFalse(circuitBreaker.currentState().isClosed());

      // should be closed when count increased -> old gc bean is closing
      when(youngerGcBean.getCollectionCount()).thenReturn(3l);
      assertTrue(circuitBreaker.currentState().isClosed());
    }
  }
}
