/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.circuitBreaker;

import java.util.concurrent.Callable;
import lombok.extern.slf4j.Slf4j;

/** Non-closing circuit breaker which always runs callable. */
@Slf4j
public class NoOpCircuitBreaker extends ExecutorCircuitBreaker {

  public NoOpCircuitBreaker() {
    super(0);
  }

  @Override
  public CircuitBreakerState currentState() {
    return new CircuitBreakerState(false);
  }

  @Override
  public <T> T run(Callable<T> callable) {
    try {
      return callable.call();
    } catch (Exception e) {
      log.error("OpenLineage callable failed to execute. Swallowing the exception {}", e);
      return null;
    }
  }
}
