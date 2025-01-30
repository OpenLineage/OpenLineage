/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.circuitBreaker;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.Callable;
import org.junit.jupiter.api.Test;

class TaskQueueCircuitBreakerTest {
  TaskQueueCircuitBreaker circuitBreaker =
      new TaskQueueCircuitBreaker(new TaskQueueCircuitBreakerConfig(1, 1, 1L, 1L, 200));

  @Test
  void testTaskQueueBasedExecution() {
    Callable<Object> infiniteCallable =
        () -> {
          while (true) Thread.sleep(1000);
        };
    assertThat(circuitBreaker.<Object>run(infiniteCallable)).isNull();
    assertThat(circuitBreaker.<Object>run(infiniteCallable)).isNull();
    assertThat(circuitBreaker.<Object>run(infiniteCallable)).isNull();
    assertThat(circuitBreaker.getTimedoutCount()).isEqualTo(2L);
    assertThat(circuitBreaker.getDroppedCount()).isEqualTo(1L);
    assertThat(circuitBreaker.getPendingTasks()).isEqualTo(1L);
    circuitBreaker.close();
  }
}
