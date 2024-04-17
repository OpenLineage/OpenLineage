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
@AllArgsConstructor
@ToString
@EqualsAndHashCode
@With
public final class JavaRuntimeCircuitBreakerConfig implements CircuitBreakerConfig {
  @Getter @Setter private Integer memoryThreshold = 20;
  @Getter @Setter private Integer gcCpuThreshold = 10;
  @Getter @Setter private Integer circuitCheckIntervalInMillis = CIRCUIT_CHECK_INTERVAL_IN_MILLIS;
  @Getter @Setter private Integer timeoutInSeconds = null;

  public JavaRuntimeCircuitBreakerConfig(
      int memoryThreshold, int gcCpuThreshold, int circuitCheckIntervalInMillis) {
    this(memoryThreshold, gcCpuThreshold, circuitCheckIntervalInMillis, null);
  }

  public JavaRuntimeCircuitBreakerConfig(int memoryThreshold, int gcCpuThreshold) {
    this(memoryThreshold, gcCpuThreshold, CIRCUIT_CHECK_INTERVAL_IN_MILLIS);
  }
}
