/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.circuitBreaker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import io.openlineage.client.circuitBreaker.CircuitBreakerResolver.CircuitBreakerServiceLoader;
import java.util.Collections;
import java.util.ServiceLoader;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

class CircuitBreakerResolverTest {

  @Test
  void testBuilderLoadedWithServiceLoader() {
    CircuitBreakerBuilder builder = mock(CircuitBreakerBuilder.class);
    CircuitBreakerConfig config = mock(CircuitBreakerConfig.class);
    CircuitBreaker circuitBreaker = mock(CircuitBreaker.class);
    when(builder.getConfig()).thenReturn(config);
    when(builder.build(config)).thenReturn(circuitBreaker);

    ServiceLoader<CircuitBreakerBuilder> serviceLoader = mock(ServiceLoader.class);
    try (MockedStatic loader = mockStatic(CircuitBreakerServiceLoader.class)) {
      when(CircuitBreakerServiceLoader.load()).thenReturn(serviceLoader);
      when(serviceLoader.spliterator())
          .thenReturn(Collections.singletonList(builder).spliterator());

      assertThat(CircuitBreakerResolver.resolveCircuitBreakerByConfig(config))
          .isEqualTo(circuitBreaker);
    }
  }

  @Test
  void testBuilder() {
    assertThat(
            CircuitBreakerResolver.resolveCircuitBreakerByConfig(mock(CircuitBreakerConfig.class)))
        .isInstanceOf(NoOpCircuitBreaker.class);
  }

  @Test
  void testDefaultBuilder() {
    assertThat(
            CircuitBreakerResolver.resolveCircuitBreakerByConfig(mock(CircuitBreakerConfig.class)))
        .isInstanceOf(NoOpCircuitBreaker.class);
  }
}
