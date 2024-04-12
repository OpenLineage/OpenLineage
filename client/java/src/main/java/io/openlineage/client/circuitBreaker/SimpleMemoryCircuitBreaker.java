/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.circuitBreaker;

import static io.openlineage.client.utils.RuntimeUtils.freeMemory;
import static io.openlineage.client.utils.RuntimeUtils.maxMemory;
import static io.openlineage.client.utils.RuntimeUtils.totalMemory;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SimpleMemoryCircuitBreaker extends ExecutorCircuitBreaker {
  private final SimpleMemoryCircuitBreakerConfig config;

  public SimpleMemoryCircuitBreaker(@NonNull final SimpleMemoryCircuitBreakerConfig config) {
    super(config.getCircuitCheckIntervalInMillis());
    this.config = config;
  }

  @Override
  public CircuitBreakerState currentState() {
    if (!isPercentageValueValid(config.getMemoryThreshold())) {
      log.warn("Invalid memory threshold configured {}", config.getMemoryThreshold());
      return new CircuitBreakerState(false);
    }

    double percentageFreeMemory =
        100 * ((freeMemory() + (maxMemory() - totalMemory())) / (double) maxMemory());
    int freeMemoryThreshold = config.getMemoryThreshold();
    log.debug(
        "Circuit breaker: percentage free memory {}%  (freeMemoryThreshold {})",
        percentageFreeMemory, freeMemoryThreshold);

    if (percentageFreeMemory <= freeMemoryThreshold) {
      String reason =
          String.format(
              "Circuit breaker tripped at memory %.2f%% (freeMemoryThreshold %d%%)",
              percentageFreeMemory, freeMemoryThreshold);
      return new CircuitBreakerState(true, reason);
    }
    return new CircuitBreakerState(false);
  }
}
