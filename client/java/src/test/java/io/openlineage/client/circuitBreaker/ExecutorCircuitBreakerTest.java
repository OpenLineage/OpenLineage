/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.circuitBreaker;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

class ExecutorCircuitBreakerTest {

  SampleExecutorCircuitBreaker circuitBreaker = new SampleExecutorCircuitBreaker();

  @Test
  @Timeout(value = 10, unit = TimeUnit.SECONDS)
  void testTimeoutForInfiniteCallable() {
    Callable<Object> infiniteCallable =
        () -> {
          while (true) Thread.sleep(1000);
        };
    assertThat(circuitBreaker.<Object>run(infiniteCallable)).isNull();
  }

  static class SampleExecutorCircuitBreaker extends ExecutorCircuitBreaker {

    CircuitBreakerState openStater = new CircuitBreakerState(false, Optional.empty());

    SampleExecutorCircuitBreaker() {
      super(100, Duration.ofMillis(500));
    }

    @Override
    public CircuitBreakerState currentState() {
      return openStater;
    }
  }
}
