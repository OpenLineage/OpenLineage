/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.client.circuitBreaker;

import static io.openlineage.client.circuitBreaker.CircuitBreaker.CIRCUIT_CHECK_INTERVAL_IN_MILLIS;

import io.openlineage.client.MergeConfig;
import java.time.Duration;
import java.util.Optional;
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
public class TimeoutCircuitBreakerConfig
    implements CircuitBreakerConfig, MergeConfig<TimeoutCircuitBreakerConfig> {

  @Getter @Setter private Integer timeoutInSeconds;
  @Getter @Setter private Integer circuitCheckIntervalInMillis = CIRCUIT_CHECK_INTERVAL_IN_MILLIS;

  public TimeoutCircuitBreakerConfig(int timeoutInSeconds) {
    this(timeoutInSeconds, CIRCUIT_CHECK_INTERVAL_IN_MILLIS);
  }

  public TimeoutCircuitBreakerConfig(int timeoutInSeconds, int circuitCheckIntervalInMillis) {
    this.timeoutInSeconds = timeoutInSeconds;
    this.circuitCheckIntervalInMillis = circuitCheckIntervalInMillis;
  }

  @Override
  public TimeoutCircuitBreakerConfig mergeWithNonNull(TimeoutCircuitBreakerConfig other) {
    return new TimeoutCircuitBreakerConfig(
        mergePropertyWith(timeoutInSeconds, other.timeoutInSeconds),
        mergeWithDefaultValue(
            circuitCheckIntervalInMillis,
            other.circuitCheckIntervalInMillis,
            CIRCUIT_CHECK_INTERVAL_IN_MILLIS));
  }

  @Override
  public Optional<Duration> getTimeout() {
    return Optional.of(Duration.ofSeconds(timeoutInSeconds));
  }
}
