/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.circuitBreaker;

import static io.openlineage.client.circuitBreaker.CircuitBreaker.CIRCUIT_CHECK_INTERVAL_IN_MILLIS;

import io.openlineage.client.MergeConfig;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@NoArgsConstructor
@ToString
@EqualsAndHashCode
@AllArgsConstructor
public final class SimpleMemoryCircuitBreakerConfig
    implements CircuitBreakerConfig, MergeConfig<SimpleMemoryCircuitBreakerConfig> {

  public static final int DEFAULT_MEMORY_THRESHOLD = 20;
  @Getter @Setter private Integer memoryThreshold = DEFAULT_MEMORY_THRESHOLD;
  @Getter @Setter private Integer circuitCheckIntervalInMillis = CIRCUIT_CHECK_INTERVAL_IN_MILLIS;
  @Getter @Setter private Integer timeoutInSeconds = null;

  public SimpleMemoryCircuitBreakerConfig(int memoryThreshold) {
    this(memoryThreshold, CIRCUIT_CHECK_INTERVAL_IN_MILLIS, null);
  }

  public SimpleMemoryCircuitBreakerConfig(int memoryThreshold, int circuitCheckIntervalInMillis) {
    this(memoryThreshold, circuitCheckIntervalInMillis, null);
  }

  @Override
  public SimpleMemoryCircuitBreakerConfig mergeWithNonNull(SimpleMemoryCircuitBreakerConfig other) {
    return new SimpleMemoryCircuitBreakerConfig(
        mergeWithDefaultValue(memoryThreshold, other.memoryThreshold, DEFAULT_MEMORY_THRESHOLD),
        mergeWithDefaultValue(
            circuitCheckIntervalInMillis,
            other.circuitCheckIntervalInMillis,
            CIRCUIT_CHECK_INTERVAL_IN_MILLIS));
  }
}
