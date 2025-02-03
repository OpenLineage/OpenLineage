/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.circuitBreaker;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.RunFacet;
import java.util.Map;
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

  /**
   * RunFacets created by a circuit breaker which may be used to contain additional metadata on
   * OpenLineage circuit breaker execution. For example, they may expose statistics on the circuit
   * breaker being closed.
   *
   * @return Map
   */
  Map<String, RunFacet> getRunFacets(OpenLineage openLineage);

  default int getCheckIntervalMillis() {
    return CIRCUIT_CHECK_INTERVAL_IN_MILLIS;
  }
}
