/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.circuitBreaker;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import lombok.NonNull;

/**
 * Static circuit breaker used mainly for the integration tests. Its config contains a sequence of
 * boolean values determining behaviour of isClosed method in a sequence of the calls.
 */
public class StaticCircuitBreaker extends ExecutorCircuitBreaker {

  private final StaticCircuitBreakerConfig config;
  private int currentIndex;

  public StaticCircuitBreaker(@NonNull final StaticCircuitBreakerConfig config) {
    super(config.getCircuitCheckIntervalInMillis());
    this.config = config;
    this.currentIndex = 0;
  }

  @Override
  public CircuitBreakerState currentState() {
    if (currentIndex >= getValuesAsList().size()) {
      return new CircuitBreakerState(true);
    }
    currentIndex++;
    return new CircuitBreakerState(getValuesAsList().get(currentIndex - 1));
  }

  private List<Boolean> getValuesAsList() {
    return Arrays.stream(config.getValuesReturned().split(","))
        .map(s -> Boolean.valueOf(s))
        .collect(Collectors.toList());
  }
}
