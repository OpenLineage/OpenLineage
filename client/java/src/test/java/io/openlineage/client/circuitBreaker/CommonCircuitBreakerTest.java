/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.circuitBreaker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import io.openlineage.client.OpenLineageYaml;
import java.util.concurrent.Callable;
import org.junit.jupiter.api.Test;

class CommonCircuitBreakerTest {

  OpenLineageYaml openLineageYaml = mock(OpenLineageYaml.class);
  Callable<Object> callableWithException =
      (() -> {
        throw new RuntimeException("This should not happen with closed circuit breaker");
      });

  @Test
  void testCallableNotRunWhenCircuitBreakerClosed() {
    CircuitBreaker circuitBreaker =
        new CircuitBreakerFactory(new StaticCircuitBreakerConfig("true")).build();
    assertThat(circuitBreaker.run(callableWithException)).isNull();
  }

  @Test
  void verifyNoExecutorServiceIsRunForNoOpCircuitBreaker() {
    CircuitBreaker circuitBreaker = new CircuitBreakerFactory(null).build();
    Callable<Integer> callable = (() -> 1);
    assertThat(circuitBreaker.run(callable)).isEqualTo(1);
  }

  @Test
  void verifyExceptionInCallableIsSwallowed() {
    CircuitBreaker circuitBreaker = new CircuitBreakerFactory(null).build();
    assertThat(circuitBreaker.run(callableWithException)).isNull();
  }

  @Test
  void verifyCircuitBreakerInterruptsCallable() {
    CircuitBreaker circuitBreaker =
        new CircuitBreakerFactory(new StaticCircuitBreakerConfig("false,false,true", 50)).build();
    Callable<Object> longLastingCallable =
        (() -> {
          Thread.sleep(2000);
          return null;
        });

    long millisBefore = System.currentTimeMillis();
    circuitBreaker.run(longLastingCallable);
    long millisAfter = System.currentTimeMillis();

    assertThat(millisAfter - millisBefore).isGreaterThanOrEqualTo(50); // proves callable started
    assertThat(millisAfter - millisBefore).isLessThan(2000); // proves callable has been interrupted
  }

  @Test
  void verifyCallableSucceeds() {
    CircuitBreaker circuitBreaker =
        new CircuitBreakerFactory(new StaticCircuitBreakerConfig("false,false,false,false", 50))
            .build();
    Callable<Integer> longLastingCallable =
        (() -> {
          Thread.sleep(100);
          return 1;
        });

    assertThat(circuitBreaker.run(longLastingCallable)).isEqualTo(1);
  }
}
