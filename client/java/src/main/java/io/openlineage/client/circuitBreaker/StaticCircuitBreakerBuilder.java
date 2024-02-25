/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.circuitBreaker;

public class StaticCircuitBreakerBuilder implements CircuitBreakerBuilder {

  @Override
  public String getType() {
    return "static";
  }

  @Override
  public CircuitBreakerConfig getConfig() {
    return new StaticCircuitBreakerConfig();
  }

  @Override
  public CircuitBreaker build(CircuitBreakerConfig config) {
    return new StaticCircuitBreaker((StaticCircuitBreakerConfig) config);
  }
}
