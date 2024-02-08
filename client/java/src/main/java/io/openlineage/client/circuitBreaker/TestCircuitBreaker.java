/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.circuitBreaker;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import lombok.NonNull;

public class TestCircuitBreaker extends CommonCircuitBreaker {

  private final TestCircuitBreakerConfig config;
  private int currentIndex;

  public TestCircuitBreaker(@NonNull final TestCircuitBreakerConfig config) {
    super(config.getCircuitCheckIntervalInMillis());
    this.config = config;
    this.currentIndex = 0;
  }

  @Override
  public boolean isClosed() {
    if (currentIndex >= getValuesAsList().size()) {
      return true;
    }
    currentIndex++;
    return getValuesAsList().get(currentIndex - 1);
  }

  private List<Boolean> getValuesAsList() {
    return Arrays.stream(config.getValuesReturned().split(","))
        .map(s -> Boolean.valueOf(s))
        .collect(Collectors.toList());
  }

  @Override
  public String getType() {
    return "test";
  }
}
