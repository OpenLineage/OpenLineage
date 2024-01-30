/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.circuitBreaker;

import lombok.NonNull;

public class CircuitBreakerFactory {

  private final CircuitBreakerConfig circuitBreakerConfig;

  public CircuitBreakerFactory(@NonNull final CircuitBreakerConfig circuitBreakerConfig) {
    this.circuitBreakerConfig = circuitBreakerConfig;
  }

  public CircuitBreaker build() {
    if (circuitBreakerConfig instanceof SimpleJvmCircuitBreakerConfig) {
      return new SimpleJvmCircuitBreaker((SimpleJvmCircuitBreakerConfig) circuitBreakerConfig);
    } else if (circuitBreakerConfig instanceof TestCircuitBreakerConfig) {
      return new TestCircuitBreaker((TestCircuitBreakerConfig) circuitBreakerConfig);
    }
    throw new UnsupportedOperationException(
        "Unsupported circuit breaker config provided " + circuitBreakerConfig);
  }
}
