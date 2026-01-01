/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.circuitBreaker;

public interface CircuitBreakerBuilder {

  String getType();

  CircuitBreakerConfig getConfig();

  CircuitBreaker build(CircuitBreakerConfig config);
}
