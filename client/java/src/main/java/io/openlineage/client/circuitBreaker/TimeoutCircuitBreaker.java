/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.client.circuitBreaker;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TimeoutCircuitBreaker extends ExecutorCircuitBreaker {

  public TimeoutCircuitBreaker(TimeoutCircuitBreakerConfig config) {
    super(config.getCircuitCheckIntervalInMillis(), config.getTimeout().get());
  }

  @Override
  public CircuitBreakerState currentState() {
    return new CircuitBreakerState(false);
  }
}
