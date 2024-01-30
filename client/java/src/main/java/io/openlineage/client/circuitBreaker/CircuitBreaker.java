/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.circuitBreaker;

public interface CircuitBreaker {

  boolean isClosed();

  default boolean isOpen() {
    return !isClosed();
  }

  String getType();
}
