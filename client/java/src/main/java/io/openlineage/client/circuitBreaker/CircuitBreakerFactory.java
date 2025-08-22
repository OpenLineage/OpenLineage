/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.circuitBreaker;

public class CircuitBreakerFactory {

  private final CircuitBreakerConfig circuitBreakerConfig;

  public CircuitBreakerFactory(final CircuitBreakerConfig circuitBreakerConfig) {
    this.circuitBreakerConfig = circuitBreakerConfig;
  }

  public CircuitBreaker build() {
    if (circuitBreakerConfig instanceof JavaRuntimeCircuitBreakerConfig) {
      return new JavaRuntimeCircuitBreaker((JavaRuntimeCircuitBreakerConfig) circuitBreakerConfig);
    } else if (circuitBreakerConfig instanceof SimpleMemoryCircuitBreakerConfig) {
      return new SimpleMemoryCircuitBreaker(
          (SimpleMemoryCircuitBreakerConfig) circuitBreakerConfig);
    } else if (circuitBreakerConfig instanceof StaticCircuitBreakerConfig) {
      return new StaticCircuitBreaker((StaticCircuitBreakerConfig) circuitBreakerConfig);
    } else if (circuitBreakerConfig instanceof TaskQueueCircuitBreakerConfig) {
      return new TaskQueueCircuitBreaker((TaskQueueCircuitBreakerConfig) circuitBreakerConfig);
    } else if (circuitBreakerConfig instanceof TimeoutCircuitBreakerConfig) {
      return new TimeoutCircuitBreaker((TimeoutCircuitBreakerConfig) circuitBreakerConfig);
    }

    return new NoOpCircuitBreaker();
  }
}
