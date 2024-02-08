/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.circuitBreaker;

import java.util.concurrent.Callable;

public interface CircuitBreaker {

  int CIRCUIT_CHECK_INTERVAL_IN_MILLIS = 1000;

  boolean isClosed();

  default boolean isOpen() {
    return !isClosed();
  }

  /**
   * Runs callable and breaks it when circuit breaker is closed
   *
   * @param callable
   * @return
   * @param <T>
   */
  <T> T run(Callable<T> callable);

  default int getCheckIntervalMillis() {
    return CIRCUIT_CHECK_INTERVAL_IN_MILLIS;
  }

  String getType();
}
