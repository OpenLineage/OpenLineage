/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.circuitBreaker;

import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class CommonCircuitBreaker implements CircuitBreaker {

  private Integer circuitCheckIntervalInMillis = CIRCUIT_CHECK_INTERVAL_IN_MILLIS;

  public CommonCircuitBreaker(Integer circuitCheckIntervalInMillis) {
    this.circuitCheckIntervalInMillis = circuitCheckIntervalInMillis;
  }

  public <T> T run(Callable<T> callable) {
    if (isClosed()) {
      log.warn("CircuitBreaker closed preventing callable to be run: {}", this);
      return null;
    }

    // TODO: this method shall implement timeout in future
    ExecutorService executor = Executors.newCachedThreadPool();
    Future<T> futureOpenLineage = executor.submit(callable);
    Future<T> futureCircuitBreaker =
        executor.submit(
            () -> {
              log.debug(
                  "Starting CircuitBreaker in background {} with interval {}",
                  this,
                  getCheckIntervalMillis());
              while (isOpen()) {
                Thread.sleep(getCheckIntervalMillis());
              }
              log.warn("CircuitBreaker cancelling OpenLineage code");
              futureOpenLineage.cancel(true); // interrupt other thread
              return null;
            });

    T result;
    try {
      result = futureOpenLineage.get();
      log.debug("Callable successfully executed. Stopping CircuitBreaker.");
      futureCircuitBreaker.cancel(true);
    } catch (ExecutionException | InterruptedException | CancellationException e) {
      futureOpenLineage.cancel(true);
      futureCircuitBreaker.cancel(true);
      log.debug("Got error in safelyRun callable: {}", e.getMessage(), e.getCause());
      result = null;
    } finally {
      executor.shutdownNow();
    }
    return result;
  }

  public int getCheckIntervalMillis() {
    return circuitCheckIntervalInMillis;
  }

  protected boolean isPercentageValueValid(Integer value) {
    return value != null && (value >= 0) && (value <= 100);
  }
}
