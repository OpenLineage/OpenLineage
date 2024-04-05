/*
/* Copyright 2018-2024 contributors to the OpenLineage project
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
@AllArgsConstructor
@ToString
@EqualsAndHashCode
public final class JavaRuntimeCircuitBreakerConfig
    implements CircuitBreakerConfig, MergeConfig<JavaRuntimeCircuitBreakerConfig> {

  public static final int DEFAULT_MEMORY_THRESHOLD = 20;
  public static final int DEFAULT_GC_CPU_THRESHOLD = 10;
  @Getter @Setter private Integer memoryThreshold = DEFAULT_MEMORY_THRESHOLD;
  @Getter @Setter private Integer gcCpuThreshold = DEFAULT_GC_CPU_THRESHOLD;
  @Getter @Setter private Integer circuitCheckIntervalInMillis = CIRCUIT_CHECK_INTERVAL_IN_MILLIS;
  @Getter @Setter private Integer timeoutInSeconds = null;

  public JavaRuntimeCircuitBreakerConfig(
      int memoryThreshold, int gcCpuThreshold, int circuitCheckIntervalInMillis) {
    this(memoryThreshold, gcCpuThreshold, circuitCheckIntervalInMillis, null);
  }

  public JavaRuntimeCircuitBreakerConfig(int memoryThreshold, int gcCpuThreshold) {
    this(memoryThreshold, gcCpuThreshold, CIRCUIT_CHECK_INTERVAL_IN_MILLIS);
  }

  @Override
  public JavaRuntimeCircuitBreakerConfig mergeWithNonNull(JavaRuntimeCircuitBreakerConfig other) {
    return new JavaRuntimeCircuitBreakerConfig(
        mergeWithDefaultValue(memoryThreshold, other.memoryThreshold, DEFAULT_MEMORY_THRESHOLD),
        mergeWithDefaultValue(gcCpuThreshold, other.gcCpuThreshold, DEFAULT_GC_CPU_THRESHOLD),
        mergeWithDefaultValue(
            circuitCheckIntervalInMillis,
            other.circuitCheckIntervalInMillis,
            CIRCUIT_CHECK_INTERVAL_IN_MILLIS));
  }
}
