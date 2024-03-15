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
public abstract class ExecutorCircuitBreaker implements CircuitBreaker {

  private Integer circuitCheckIntervalInMillis;

  public ExecutorCircuitBreaker(Integer circuitCheckIntervalInMillis) {
    this.circuitCheckIntervalInMillis = circuitCheckIntervalInMillis;
  }

  @Override
  public <T> T run(Callable<T> callable) {
    if (currentState().isClosed()) {
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
              CircuitBreakerState circuitBreakerState = currentState();
              while (!circuitBreakerState.isClosed()) {
                Thread.sleep(getCheckIntervalMillis());
                circuitBreakerState = currentState();
              }
              log.warn(
                  "CircuitBreaker cancelling OpenLineage code: " + circuitBreakerState.getReason());
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
      log.warn("Got error in run callable: {}", e.getMessage(), e.getCause());
      executor.shutdownNow();
      return null;
    } finally {
      executor.shutdownNow();
    }
    return result;
  }

  @Override
  public int getCheckIntervalMillis() {
    return circuitCheckIntervalInMillis;
  }

  protected boolean isPercentageValueValid(Integer value) {
    return value != null && (value >= 0) && (value <= 100);
  }
}
