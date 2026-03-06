/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class StreamingMicroBatchThrottlerTest {

  private static final String JOB_A = "ns/jobA";
  private static final String JOB_B = "ns/jobB";

  @Test
  void neitherConfiguredAlwaysEmits() {
    StreamingMicroBatchThrottler t = new StreamingMicroBatchThrottler(null, null);
    for (int i = 0; i < 10; i++) {
      assertThat(t.shouldEmit(JOB_A)).as("batch %d should emit", i).isTrue();
    }
  }

  @Test
  void countThrottleEmitsEveryNthBatch() {
    StreamingMicroBatchThrottler t = new StreamingMicroBatchThrottler(3, null);
    // batch 0 → emit (0 % 3 == 0)
    assertThat(t.shouldEmit(JOB_A)).isTrue();
    // batch 1 → skip
    assertThat(t.shouldEmit(JOB_A)).isFalse();
    // batch 2 → skip
    assertThat(t.shouldEmit(JOB_A)).isFalse();
    // batch 3 → emit (3 % 3 == 0)
    assertThat(t.shouldEmit(JOB_A)).isTrue();
    // batch 4 → skip
    assertThat(t.shouldEmit(JOB_A)).isFalse();
    // batch 5 → skip
    assertThat(t.shouldEmit(JOB_A)).isFalse();
    // batch 6 → emit
    assertThat(t.shouldEmit(JOB_A)).isTrue();
  }

  @Test
  void countThrottleOf1AlwaysEmits() {
    StreamingMicroBatchThrottler t = new StreamingMicroBatchThrottler(1, null);
    for (int i = 0; i < 5; i++) {
      assertThat(t.shouldEmit(JOB_A)).as("batch %d should emit", i).isTrue();
    }
  }

  @Test
  void countThrottleOfZeroThrowsIllegalArgumentException() {
    assertThatThrownBy(() -> new StreamingMicroBatchThrottler(0, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("microbatchThrottle must be >= 1");
  }

  @Test
  void countThrottleNegativeThrowsIllegalArgumentException() {
    assertThatThrownBy(() -> new StreamingMicroBatchThrottler(-5, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("microbatchThrottle must be >= 1");
  }

  @Test
  void timeThrottleEmitsOnFirstBatch() {
    // Very large threshold (100 minutes) — first batch always emits because lastEmitTime is null.
    StreamingMicroBatchThrottler t = new StreamingMicroBatchThrottler(null, 100);
    assertThat(t.shouldEmit(JOB_A)).isTrue();
    // second call: lastEmitTime is now set, 0 minutes have elapsed, threshold not met
    assertThat(t.shouldEmit(JOB_A)).isFalse();
  }

  @Test
  void timeThrottleEmitsAfterElapsed() {
    // Use a controllable clock to simulate time passage.
    AtomicReference<Instant> fakeNow = new AtomicReference<>(Instant.parse("2024-01-01T00:00:00Z"));
    StreamingMicroBatchThrottler t = new StreamingMicroBatchThrottler(null, 5, fakeNow::get);

    // batch 0: lastEmitTime is null → emit
    assertThat(t.shouldEmit(JOB_A)).isTrue();

    // Advance clock by 3 minutes (below 5-minute threshold) → should NOT emit
    fakeNow.set(fakeNow.get().plus(3, ChronoUnit.MINUTES));
    assertThat(t.shouldEmit(JOB_A)).isFalse();

    // Advance clock by another 3 minutes (total 6 minutes elapsed since last emit) → should emit
    fakeNow.set(fakeNow.get().plus(3, ChronoUnit.MINUTES));
    assertThat(t.shouldEmit(JOB_A)).isTrue();

    // Immediately after emit, threshold not met again → should NOT emit
    assertThat(t.shouldEmit(JOB_A)).isFalse();
  }

  @Test
  void timeThrottleZeroMinutesAlwaysEmits() {
    // threshold = 0 minutes — every call satisfies >= 0
    StreamingMicroBatchThrottler t = new StreamingMicroBatchThrottler(null, 0);
    for (int i = 0; i < 5; i++) {
      assertThat(t.shouldEmit(JOB_A)).as("batch %d should emit", i).isTrue();
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
    assertThat(t.shouldEmit(JOB_A)).isTrue(); // batch 0: count satisfied
    assertThat(t.shouldEmit(JOB_A)).isFalse(); // batch 1: neither
    assertThat(t.shouldEmit(JOB_A)).isFalse(); // batch 2: neither
    assertThat(t.shouldEmit(JOB_A)).isTrue(); // batch 3: count satisfied
    assertThat(t.shouldEmit(JOB_A)).isFalse(); // batch 4: neither
    assertThat(t.shouldEmit(JOB_A)).isFalse(); // batch 5: neither
    assertThat(t.shouldEmit(JOB_A)).isTrue(); // batch 6: count satisfied
  }

  @Test
  void bothConfiguredTimeConditionTriggers() {
    // N=10 (count rarely satisfied), M=5 minutes.
    // Use controllable clock so time condition triggers independently.
    AtomicReference<Instant> fakeNow = new AtomicReference<>(Instant.parse("2024-01-01T00:00:00Z"));
    StreamingMicroBatchThrottler t = new StreamingMicroBatchThrottler(10, 5, fakeNow::get);

    // batch 0: count 0%10==0 → emit
    assertThat(t.shouldEmit(JOB_A)).isTrue();

    // batch 1-2: neither count nor time satisfied
    assertThat(t.shouldEmit(JOB_A)).isFalse();
    assertThat(t.shouldEmit(JOB_A)).isFalse();

    // Advance clock past 5 minutes → time condition now satisfied
    fakeNow.set(fakeNow.get().plus(6, ChronoUnit.MINUTES));

    // batch 3: time condition triggers (OR semantics)
    assertThat(t.shouldEmit(JOB_A)).isTrue();

    // batch 4: time reset, count not satisfied → skip
    assertThat(t.shouldEmit(JOB_A)).isFalse();
  }

  @Test
  void lastEmitTimeUpdatedOnEmit() {
    // N=2, M=100 minutes — time condition is never re-satisfied after first emit in a test
    StreamingMicroBatchThrottler t = new StreamingMicroBatchThrottler(2, 100);
    // batch 0 → count 0%2==0 → emit, lastEmitTime set
    assertThat(t.shouldEmit(JOB_A)).isTrue();
    // batch 1 → count 1%2!=0, time elapsed < 100 min → skip
    assertThat(t.shouldEmit(JOB_A)).isFalse();
    // batch 2 → count 2%2==0 → emit, lastEmitTime updated
    assertThat(t.shouldEmit(JOB_A)).isTrue();
    // batch 3 → count 3%2!=0, time elapsed < 100 min → skip
    assertThat(t.shouldEmit(JOB_A)).isFalse();
  }

  @Test
  void differentJobsHaveIndependentThrottleState() {
    // N=3 — each job gets its own counter starting at 0
    StreamingMicroBatchThrottler t = new StreamingMicroBatchThrottler(3, null);
    // jobA: batch 0 → emit
    assertThat(t.shouldEmit(JOB_A)).isTrue();
    // jobB: batch 0 → emit (independent counter)
    assertThat(t.shouldEmit(JOB_B)).isTrue();
    // jobA: batch 1 → skip
    assertThat(t.shouldEmit(JOB_A)).isFalse();
    // jobB: batch 1 → skip
    assertThat(t.shouldEmit(JOB_B)).isFalse();
    // jobA: batch 2 → skip
    assertThat(t.shouldEmit(JOB_A)).isFalse();
    // jobA: batch 3 → emit
    assertThat(t.shouldEmit(JOB_A)).isTrue();
    // jobB: batch 2 → skip (still on its own counter)
    assertThat(t.shouldEmit(JOB_B)).isFalse();
  }
}
