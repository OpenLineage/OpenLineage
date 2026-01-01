/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.circuitBreaker;

public class JavaRuntimeCircuitBreakerBuilder implements CircuitBreakerBuilder {

  @Override
  public String getType() {
    return "javaRuntime";
  }

  @Override
  public CircuitBreakerConfig getConfig() {
    return new JavaRuntimeCircuitBreakerConfig();
  }

  @Override
  public CircuitBreaker build(CircuitBreakerConfig config) {
    return new JavaRuntimeCircuitBreaker((JavaRuntimeCircuitBreakerConfig) config);
  }
}
