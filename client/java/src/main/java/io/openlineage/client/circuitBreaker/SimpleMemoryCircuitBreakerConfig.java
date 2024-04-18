/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.circuitBreaker;

import static io.openlineage.client.circuitBreaker.CircuitBreaker.CIRCUIT_CHECK_INTERVAL_IN_MILLIS;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.With;

@NoArgsConstructor
@ToString
@EqualsAndHashCode
@AllArgsConstructor
@With
public final class SimpleMemoryCircuitBreakerConfig implements CircuitBreakerConfig {
  @Getter @Setter private Integer memoryThreshold = 20;
  @Getter @Setter private Integer circuitCheckIntervalInMillis = CIRCUIT_CHECK_INTERVAL_IN_MILLIS;
  @Getter @Setter private Integer timeoutInSeconds = null;

  public SimpleMemoryCircuitBreakerConfig(int memoryThreshold) {
    this(memoryThreshold, CIRCUIT_CHECK_INTERVAL_IN_MILLIS, null);
  }

  public SimpleMemoryCircuitBreakerConfig(int memoryThreshold, int circuitCheckIntervalInMillis) {
    this(memoryThreshold, circuitCheckIntervalInMillis, null);
  }
}
