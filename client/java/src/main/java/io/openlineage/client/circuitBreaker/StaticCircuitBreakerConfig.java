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

@NoArgsConstructor
@ToString
@EqualsAndHashCode
@AllArgsConstructor
public final class StaticCircuitBreakerConfig implements CircuitBreakerConfig {
  @Getter @Setter private String valuesReturned;
  @Getter @Setter private Integer circuitCheckIntervalInMillis = CIRCUIT_CHECK_INTERVAL_IN_MILLIS;

  public StaticCircuitBreakerConfig(String valuesReturned) {
    this(valuesReturned, CIRCUIT_CHECK_INTERVAL_IN_MILLIS);
  }
}
