/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.util;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;

/**
 * Stateful throttler for Spark Structured Streaming micro-batch events.
 *
 * <p>Structured Streaming emits a START + COMPLETE event pair for every micro-batch. At a 10-second
 * trigger interval, that is roughly 360 events per hour indefinitely. This class reduces volume by
 * allowing either a count-based throttle (every N micro-batches), a time-based throttle (every M
 * minutes), or both with OR semantics: emit when either condition is satisfied.
 *
 * <p>Configuration:
 *
 * <ul>
 *   <li>{@code microbatchThrottle=N} — emit every Nth batch (batch 0, N, 2N, ...)
 *   <li>{@code microbatchThrottleMinutes=M} — emit if at least M minutes have passed since last
 *       emit
 *   <li>Both set — emit if EITHER condition is met
 *   <li>Neither set — always emit (no-op, backward compatible)
 * </ul>
 */
@Slf4j
public class StreamingMicroBatchThrottler {

  private final AtomicLong batchCount = new AtomicLong(0);
  private final AtomicReference<Instant> lastEmitTime = new AtomicReference<>(null);
  private final Integer microbatchThrottle;
  private final Integer microbatchThrottleMinutes;

  public StreamingMicroBatchThrottler(
      Integer microbatchThrottle, Integer microbatchThrottleMinutes) {
    this.microbatchThrottle = microbatchThrottle;
    this.microbatchThrottleMinutes = microbatchThrottleMinutes;
  }

  /**
   * Returns {@code true} if this micro-batch should emit OpenLineage events. Increments the
   * internal batch counter and updates {@code lastEmitTime} when returning {@code true}.
   *
   * <p>Semantics:
   *
   * <ul>
   *   <li>Neither configured → always emit (backward compatible)
   *   <li>Only count configured → emit every Nth batch
   *   <li>Only time configured → emit when ≥M minutes have elapsed since last emit
   *   <li>Both configured → emit when EITHER condition is satisfied (OR semantics)
   * </ul>
   */
  public boolean shouldEmit() {
    long count = batchCount.getAndIncrement();
    Instant now = Instant.now();
    Instant last = lastEmitTime.get();

    boolean emit;
    boolean countOk = (microbatchThrottle != null) && (count % microbatchThrottle == 0);
    boolean timeOk =
        (microbatchThrottleMinutes != null)
            && ((last == null)
                || (Duration.between(last, now).toMinutes() >= microbatchThrottleMinutes));

    if (microbatchThrottle == null && microbatchThrottleMinutes == null) {
      // Neither configured — always emit.
      emit = true;
    } else if (microbatchThrottle != null && microbatchThrottleMinutes != null) {
      // Both configured — OR semantics.
      emit = countOk || timeOk;
    } else if (microbatchThrottle != null) {
      // Only count-based throttle.
      emit = countOk;
    } else {
      // Only time-based throttle.
      emit = timeOk;
    }

    if (emit) {
      lastEmitTime.set(now);
    }

    log.debug(
        "StreamingMicroBatchThrottler: batch={} countOk={} timeOk={} emit={}",
        count,
        countOk,
        timeOk,
        emit);

    return emit;
  }
}
