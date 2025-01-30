/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.circuitBreaker;

import io.micrometer.common.lang.NonNull;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;

/**
 * Queues every openlineage task for execution by a bounded threadpool to prevent creation of too
 * many threads (unlike cachedthreadpool) and resulting impact on the spark job. Once queued, the
 * circuit breaker waits for some configured time for the task to finish execution in an effort to
 * preserve the ordering of completion of tasks. If queue is full, it gives up on the task. An
 * explicit close() that need to be called after the application end event is processed shuts it
 * down after waiting for a while (configurable) to finish pending tasks. Also the circuit breaker
 * maintains the count of rejected, canceled tasks, or submission timeouts.
 */
@Slf4j
public class TaskQueueCircuitBreaker implements CircuitBreaker {
  private BlockingQueue<Runnable> eventQueue;
  private ExecutorService eventProcessingExecutor;
  private Long timeoutSeconds;
  private Long shutdownTimeoutSeconds;

  private final AtomicLong dropped = new AtomicLong();
  private final AtomicLong timedOut = new AtomicLong();
  private final AtomicLong failed = new AtomicLong();

  public TaskQueueCircuitBreaker(@NonNull TaskQueueCircuitBreakerConfig config) {
    this.timeoutSeconds = config.getTimeoutSeconds();
    this.shutdownTimeoutSeconds = config.getShutdownTimeoutSeconds();
    eventQueue = new ArrayBlockingQueue<>(config.getQueueSize());
    eventProcessingExecutor =
        new ThreadPoolExecutor(
            config.getThreadCount(), config.getThreadCount(), 60L, TimeUnit.SECONDS, eventQueue);
  }

  @Override
  public CircuitBreakerState currentState() {
    return new CircuitBreakerState(false);
  }

  @Override
  public <T> T run(Callable<T> callable) {
    try {
      T result = eventProcessingExecutor.submit(callable).get(timeoutSeconds, TimeUnit.SECONDS);
      return result;
    } catch (RejectedExecutionException re) {
      dropped.incrementAndGet();
      return null;
    } catch (TimeoutException e) {
      timedOut.incrementAndGet();
      return null;
    } catch (Exception e) {
      failed.incrementAndGet();
      return null;
    } finally {
      log.info(
          "Openlineage async stats: dropped={}, timeout={}, queueDepth={}, failed={}",
          dropped.get(),
          timedOut.get(),
          getPendingTasks(),
          failed.get());
    }
  }

  public long getDroppedCount() {
    return dropped.get();
  }

  public long getFailedCount() {
    return failed.get();
  }

  public long getTimedoutCount() {
    return timedOut.get();
  }

  public int getPendingTasks() {
    return eventQueue == null ? 0 : eventQueue.size();
  }

  @Override
  public int getCheckIntervalMillis() {
    return CircuitBreaker.super.getCheckIntervalMillis();
  }

  @Override
  public void close() {
    try {
      // First just shutdown the executor. It does NOT cancel already submitted tasks, just won't
      // accept new tasks.
      eventProcessingExecutor.shutdown();
      // Wait for a shutdownWait seconds for the pending tasks to be executed. If they are not
      // executed by that time,
      // force a shutdown where the pending tasks are also abandoned.
      eventProcessingExecutor.awaitTermination(shutdownTimeoutSeconds, TimeUnit.SECONDS);
      // Force shutdown, canceling pending tasks. This will result in loss of events.
      List<Runnable> canceledTasks = eventProcessingExecutor.shutdownNow();
      dropped.addAndGet(canceledTasks.size());
    } catch (Exception e) {
      log.error("Unable to shutdown pending event processing tasks", e);
    }
    // Once pending tasks are complete/conceled, process this end event synchronously
  }
}
