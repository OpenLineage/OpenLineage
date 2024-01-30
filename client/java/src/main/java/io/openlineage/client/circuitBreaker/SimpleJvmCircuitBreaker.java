/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.circuitBreaker;

import static io.openlineage.client.circuitBreaker.SimpleJvmCircuitBreaker.CircuitBreakerRuntime.freeMemory;
import static io.openlineage.client.circuitBreaker.SimpleJvmCircuitBreaker.CircuitBreakerRuntime.maxMemory;
import static io.openlineage.client.circuitBreaker.SimpleJvmCircuitBreaker.CircuitBreakerRuntime.totalMemory;

import java.util.Optional;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SimpleJvmCircuitBreaker implements CircuitBreaker {
  private final SimpleJvmCircuitBreakerConfig config;

  public SimpleJvmCircuitBreaker(@NonNull final SimpleJvmCircuitBreakerConfig config) {
    this.config = config;
  }

  @Override
  public boolean isClosed() {
    if (config.getMemoryThreshold() == null
        || config.getMemoryThreshold() < 0
        || config.getMemoryThreshold() > 100) {
      log.warn("Invalid memory threshold configured {}", config.getMemoryThreshold());
      return false;
    }

    // https://stackoverflow.com/questions/3571203/what-are-runtime-getruntime-totalmemory-and-freememory
    return Optional.ofNullable(config.getMemoryThreshold())
        .filter(
            threshold ->
                100d * ((freeMemory() + (maxMemory() - totalMemory())) / (double) maxMemory())
                    < threshold)
        .isPresent();
  }

  @Override
  public String getType() {
    return "simple";
  }

  static class CircuitBreakerRuntime {
    static long freeMemory() {
      return Runtime.getRuntime().freeMemory();
    }

    static long totalMemory() {
      return Runtime.getRuntime().totalMemory();
    }

    static long maxMemory() {
      return Runtime.getRuntime().maxMemory();
    }
  }
}
