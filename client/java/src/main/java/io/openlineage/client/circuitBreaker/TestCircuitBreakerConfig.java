/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.circuitBreaker;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.With;

@NoArgsConstructor
@ToString
@EqualsAndHashCode
@AllArgsConstructor
@With
public final class TestCircuitBreakerConfig implements CircuitBreakerConfig {
  @Getter @Setter private boolean isClosed;
}
