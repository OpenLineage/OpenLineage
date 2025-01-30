/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.circuitBreaker;

import java.time.Duration;
import java.util.Optional;
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
  protected Optional<Duration> timeout;
  private ExecutorService executor;

  public ExecutorCircuitBreaker(Integer circuitCheckIntervalInMillis) {
    this.circuitCheckIntervalInMillis = circuitCheckIntervalInMillis;
    this.timeout = Optional.empty();
    executor = Executors.newCachedThreadPool();
  }

  public ExecutorCircuitBreaker(Integer circuitCheckIntervalInMillis, Duration timeout) {
    this.circuitCheckIntervalInMillis = circuitCheckIntervalInMillis;
    this.timeout = Optional.of(timeout);
    executor = Executors.newCachedThreadPool();
  }

  @Override
  public <T> T run(Callable<T> callable) {
    if (currentState().isClosed()) {
      log.warn("CircuitBreaker closed preventing callable to be run: {}", this);
      return null;
    }

    long startTime = System.currentTimeMillis();
    Future<T> futureOpenLineage = executor.submit(callable);
    Future<T> futureCircuitBreaker =
        executor.submit(
            () -> {
              log.debug(
                  "Starting CircuitBreaker in background {} with interval {}",
                  this,
                  getCheckIntervalMillis());
              CircuitBreakerState circuitBreakerState = currentState();
              boolean isTimeoutExceeded = false;
              while (!circuitBreakerState.isClosed() && !isTimeoutExceeded) {
                Thread.sleep(getCheckIntervalMillis());
                circuitBreakerState = currentState();

                Duration runningTime = Duration.ofMillis(System.currentTimeMillis() - startTime);
                isTimeoutExceeded =
                    timeout.map(t -> t.minus(runningTime).isNegative()).orElse(false);
              }
              if (circuitBreakerState.isClosed()) {
                log.warn(
                    "CircuitBreaker cancelling OpenLineage code: {}",
                    circuitBreakerState.getReason());
              } else {
                log.warn("CircuitBreaker timeout exceeded: {}", timeout.get());
              }
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
      return null;
    }
    return result;
  }

  @Override
  public int getCheckIntervalMillis() {
    return circuitCheckIntervalInMillis;
  }

  @Override
  public void close() {
    log.info("No-op close");
  }

  public Optional<Duration> getTimeout() {
    return timeout;
  }

  protected boolean isPercentageValueValid(Integer value) {
    return value != null && (value >= 0) && (value <= 100);
  }
}
