/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.circuitBreaker;

public class SimpleMemoryCircuitBreakerBuilder implements CircuitBreakerBuilder {

  @Override
  public String getType() {
    return "simpleMemory";
  }

  @Override
  public CircuitBreakerConfig getConfig() {
    return new SimpleMemoryCircuitBreakerConfig();
  }

  @Override
  public CircuitBreaker build(CircuitBreakerConfig config) {
    return new SimpleMemoryCircuitBreaker((SimpleMemoryCircuitBreakerConfig) config);
  }
}
