/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.circuitBreaker;

public class NoOpCircuitBreakerBuilder implements CircuitBreakerBuilder {

  @Override
  public String getType() {
    return "noop";
  }

  @Override
  public CircuitBreakerConfig getConfig() {
    return null;
  }

  @Override
  public CircuitBreaker build(CircuitBreakerConfig config) {
    return new NoOpCircuitBreaker();
  }
}
