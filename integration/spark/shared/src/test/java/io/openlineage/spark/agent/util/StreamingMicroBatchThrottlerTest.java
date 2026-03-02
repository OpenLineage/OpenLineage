/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.util;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class StreamingMicroBatchThrottlerTest {

  @Test
  void neitherConfiguredAlwaysEmits() {
    StreamingMicroBatchThrottler t = new StreamingMicroBatchThrottler(null, null);
    for (int i = 0; i < 10; i++) {
      assertThat(t.shouldEmit()).as("batch %d should emit", i).isTrue();
    }
  }

  @Test
  void countThrottleEmitsEveryNthBatch() {
    StreamingMicroBatchThrottler t = new StreamingMicroBatchThrottler(3, null);
    // batch 0 → emit (0 % 3 == 0)
    assertThat(t.shouldEmit()).isTrue();
    // batch 1 → skip
    assertThat(t.shouldEmit()).isFalse();
    // batch 2 → skip
    assertThat(t.shouldEmit()).isFalse();
    // batch 3 → emit (3 % 3 == 0)
    assertThat(t.shouldEmit()).isTrue();
    // batch 4 → skip
    assertThat(t.shouldEmit()).isFalse();
    // batch 5 → skip
    assertThat(t.shouldEmit()).isFalse();
    // batch 6 → emit
    assertThat(t.shouldEmit()).isTrue();
  }

  @Test
  void countThrottleOf1AlwaysEmits() {
    StreamingMicroBatchThrottler t = new StreamingMicroBatchThrottler(1, null);
    for (int i = 0; i < 5; i++) {
      assertThat(t.shouldEmit()).as("batch %d should emit", i).isTrue();
    }
  }

  @Test
  void timeThrottleEmitsOnFirstBatch() {
    // Very large threshold (100 minutes) — first batch always emits because lastEmitTime is null.
    StreamingMicroBatchThrottler t = new StreamingMicroBatchThrottler(null, 100);
    assertThat(t.shouldEmit()).isTrue();
    // second call: lastEmitTime is now set, 0 minutes have elapsed, threshold not met
    assertThat(t.shouldEmit()).isFalse();
  }

  @Test
  void timeThrottleZeroMinutesAlwaysEmits() {
    // threshold = 0 minutes — every call satisfies >= 0
    StreamingMicroBatchThrottler t = new StreamingMicroBatchThrottler(null, 0);
    for (int i = 0; i < 5; i++) {
      assertThat(t.shouldEmit()).as("batch %d should emit", i).isTrue();
    }
  }

  @Test
  void bothConfiguredUsesOrSemantics() {
    // N=3, M=100 minutes (time condition never satisfied after first emit)
    // batch 0 → count 0%3==0 → emit
    // batch 1 → count false, time false → skip
    // batch 2 → count false, time false → skip
    // batch 3 → count 3%3==0 → emit
    StreamingMicroBatchThrottler t = new StreamingMicroBatchThrottler(3, 100);
    assertThat(t.shouldEmit()).isTrue(); // batch 0: count satisfied
    assertThat(t.shouldEmit()).isFalse(); // batch 1: neither
    assertThat(t.shouldEmit()).isFalse(); // batch 2: neither
    assertThat(t.shouldEmit()).isTrue(); // batch 3: count satisfied
    assertThat(t.shouldEmit()).isFalse(); // batch 4: neither
    assertThat(t.shouldEmit()).isFalse(); // batch 5: neither
    assertThat(t.shouldEmit()).isTrue(); // batch 6: count satisfied
  }

  @Test
  void lastEmitTimeUpdatedOnEmit() {
    // N=2, M=100 minutes — time condition is never re-satisfied after first emit in a test
    StreamingMicroBatchThrottler t = new StreamingMicroBatchThrottler(2, 100);
    // batch 0 → count 0%2==0 → emit, lastEmitTime set
    assertThat(t.shouldEmit()).isTrue();
    // batch 1 → count 1%2!=0, time elapsed < 100 min → skip
    assertThat(t.shouldEmit()).isFalse();
    // batch 2 → count 2%2==0 → emit, lastEmitTime updated
    assertThat(t.shouldEmit()).isTrue();
    // batch 3 → count 3%2!=0, time elapsed < 100 min → skip
    assertThat(t.shouldEmit()).isFalse();
  }
}
