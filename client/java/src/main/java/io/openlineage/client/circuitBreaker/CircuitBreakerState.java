/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.circuitBreaker;

import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

@AllArgsConstructor
@EqualsAndHashCode
@ToString
public class CircuitBreakerState {
  @Getter private final boolean isClosed;
  @Getter private final Optional<String> reason;

  public CircuitBreakerState(boolean isClosed) {
    this.isClosed = isClosed;
    this.reason = Optional.empty();
  }

  public CircuitBreakerState(boolean isClosed, String reason) {
    this.isClosed = isClosed;
    this.reason = Optional.of(reason);
  }
}
