/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.circuitBreaker;

import static io.openlineage.client.circuitBreaker.RuntimeUtils.freeMemory;
import static io.openlineage.client.circuitBreaker.RuntimeUtils.maxMemory;
import static io.openlineage.client.circuitBreaker.RuntimeUtils.totalMemory;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SimpleMemoryCircuitBreaker extends CommonCircuitBreaker {
  private final SimpleMemoryCircuitBreakerConfig config;

  public SimpleMemoryCircuitBreaker(@NonNull final SimpleMemoryCircuitBreakerConfig config) {
    super(config.getCircuitCheckIntervalInMillis());
    this.config = config;
  }

  @Override
  public boolean isClosed() {
    if (!isPercentageValueValid(config.getMemoryThreshold())) {
      log.warn("Invalid memory threshold configured {}", config.getMemoryThreshold());
      return false;
    }

    double percentageFreeMemory =
        100 * ((freeMemory() + (maxMemory() - totalMemory())) / (double) maxMemory());
    int freeMemoryThreshold = config.getMemoryThreshold();
    log.debug(
        "Circuit breaker: percentage free memory {}%  (freeMemoryThreshold {})",
        percentageFreeMemory, freeMemoryThreshold);

    if (percentageFreeMemory <= freeMemoryThreshold) {
      log.warn("Circuit breaker tripped at memory {}%", percentageFreeMemory);
      return true;
    }
    return false;
  }

  @Override
  public String getType() {
    return "javaRuntime";
  }
}
