/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.util;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;

/**
 * Stateful throttler for Spark Structured Streaming micro-batch events.
 *
 * <p>Structured Streaming emits a START + COMPLETE event pair for every micro-batch. At a 10-second
 * trigger interval, that is roughly 720 events per hour indefinitely. This class reduces volume by
 * allowing either a count-based throttle (every N micro-batches), a time-based throttle (every M
 * minutes), or both with OR semantics: emit when either condition is satisfied.
 *
 * <p>Throttle state is maintained <em>per job</em> (keyed by job name + namespace) so that multiple
 * concurrent streaming queries are throttled independently. This prevents a single shared counter
 * from causing one streaming query's batch counter to affect another query's visibility.
 *
 * <p>Configuration:
 *
 * <ul>
 *   <li>{@code microbatchThrottle=N} — emit every Nth batch (batch 0, N, 2N, ...); must be &gt;= 1
 *   <li>{@code microbatchThrottleMinutes=M} — emit if at least M minutes have passed since last
 *       emit
 *   <li>Both set — emit if EITHER condition is met
 *   <li>Neither set — always emit (no-op, backward compatible)
 * </ul>
 *
 * <p>Thread-safety note: {@link JobThrottleState#shouldEmit(String)} uses {@link AtomicLong} and
 * {@link AtomicReference} individually, but the compound check-then-update is not strictly atomic.
 * Two concurrent threads could both read the same stale {@code lastEmitTime} and both emit. In
 * practice Spark delivers micro-batch events sequentially per query, so this is benign.
 */
@Slf4j
public class StreamingMicroBatchThrottler {

  private final Integer microbatchThrottle;
  private final Integer microbatchThrottleMinutes;
  private final Supplier<Instant> clock;

  /** Per-job throttle state, keyed by "{namespace}/{jobName}". */
  private final ConcurrentHashMap<String, JobThrottleState> jobStates = new ConcurrentHashMap<>();

  public StreamingMicroBatchThrottler(
      Integer microbatchThrottle, Integer microbatchThrottleMinutes) {
    this(microbatchThrottle, microbatchThrottleMinutes, Instant::now);
  }

  /** Package-private constructor for testing with a controllable clock. */
  StreamingMicroBatchThrottler(
      Integer microbatchThrottle, Integer microbatchThrottleMinutes, Supplier<Instant> clock) {
    if (microbatchThrottle != null && microbatchThrottle <= 0) {
      throw new IllegalArgumentException(
          "microbatchThrottle must be >= 1 (got " + microbatchThrottle + "); use null to disable");
    }
    this.microbatchThrottle = microbatchThrottle;
    this.microbatchThrottleMinutes = microbatchThrottleMinutes;
    this.clock = clock;
  }

  /**
   * Returns {@code true} if this micro-batch should emit OpenLineage events for the given job.
   * Increments the internal batch counter for that job and updates {@code lastEmitTime} when
   * returning {@code true}.
   *
   * <p>Semantics:
   *
   * <ul>
   *   <li>Neither configured → always emit (backward compatible)
   *   <li>Only count configured → emit every Nth batch
   *   <li>Only time configured → emit when ≥M minutes have elapsed since last emit
   *   <li>Both configured → emit when EITHER condition is satisfied (OR semantics)
   * </ul>
   *
   * @param jobKey a unique identifier for the streaming job, typically "{namespace}/{jobName}"
   */
  public boolean shouldEmit(String jobKey) {
    JobThrottleState state = jobStates.computeIfAbsent(jobKey, k -> new JobThrottleState());
    return state.shouldEmit(jobKey);
  }

  private class JobThrottleState {
    private final AtomicLong batchCount = new AtomicLong(0);
    private final AtomicReference<Instant> lastEmitTime = new AtomicReference<>(null);

    boolean shouldEmit(String jobKey) {
      long count = batchCount.getAndIncrement();
      Instant now = clock.get();
      Instant last = lastEmitTime.get();

      boolean countOk = (microbatchThrottle != null) && (count % microbatchThrottle == 0);
      boolean timeOk =
          (microbatchThrottleMinutes != null)
              && ((last == null)
                  || (Duration.between(last, now).toMinutes() >= microbatchThrottleMinutes));

      boolean emit;
      if (microbatchThrottle == null && microbatchThrottleMinutes == null) {
        // Neither configured — always emit.
        emit = true;
      } else {
        // One or both configured — OR semantics (countOk/timeOk are false when their
        // respective config is null, so this collapses all three remaining cases correctly).
        emit = countOk || timeOk;
      }

      if (emit) {
        lastEmitTime.set(now);
      }

      log.debug(
          "StreamingMicroBatchThrottler: job={} batch={} countOk={} timeOk={} emit={}",
          jobKey,
          count,
          countOk,
          timeOk,
          emit);

      return emit;
    }
  }
}
