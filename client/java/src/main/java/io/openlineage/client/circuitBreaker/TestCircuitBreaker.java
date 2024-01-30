/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.circuitBreaker;

import lombok.NonNull;

public class TestCircuitBreaker implements CircuitBreaker {

  final boolean isClosed;

  public TestCircuitBreaker(@NonNull final TestCircuitBreakerConfig config) {
    this.isClosed = config.isClosed();
  }

  @Override
  public boolean isClosed() {
    return isClosed;
  }

  @Override
  public String getType() {
    return "test";
  }
}
