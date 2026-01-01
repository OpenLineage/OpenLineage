/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.circuitBreaker;

import java.util.concurrent.Callable;

public interface CircuitBreaker {

  int CIRCUIT_CHECK_INTERVAL_IN_MILLIS = 1000;

  CircuitBreakerState currentState();

  /**
   * @param callable The callable to be run
   * @return result of callable
   * @param <T> callable generic type
   */
  <T> T run(Callable<T> callable);

  void close();

  default int getCheckIntervalMillis() {
    return CIRCUIT_CHECK_INTERVAL_IN_MILLIS;
  }
}
