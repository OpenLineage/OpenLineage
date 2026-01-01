/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.circuitBreaker;

public class TimeoutCircuitBreakerBuilder implements CircuitBreakerBuilder {

  @Override
  public String getType() {
    return "timeout";
  }

  @Override
  public CircuitBreakerConfig getConfig() {
    return new TimeoutCircuitBreakerConfig();
  }

  @Override
  public CircuitBreaker build(CircuitBreakerConfig config) {
    return new TimeoutCircuitBreaker((TimeoutCircuitBreakerConfig) config);
  }
}
