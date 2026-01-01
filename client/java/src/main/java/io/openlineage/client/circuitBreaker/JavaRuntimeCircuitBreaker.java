/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.circuitBreaker;

import static io.openlineage.client.utils.RuntimeUtils.freeMemory;
import static io.openlineage.client.utils.RuntimeUtils.getGarbageCollectorMXBeans;
import static io.openlineage.client.utils.RuntimeUtils.maxMemory;
import static io.openlineage.client.utils.RuntimeUtils.totalMemory;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class JavaRuntimeCircuitBreaker extends ExecutorCircuitBreaker {
  private volatile GarbageCollectorMXBean oldGenGCBeanCached = null;
  private final JavaRuntimeCircuitBreakerConfig config;

  private final ThreadLocal<Long> lastTotalGCTimeNS =
      new ThreadLocal<Long>() {
        @Override
        protected Long initialValue() {
          return getGCCpuTimeNS();
        }
      };
  private final ThreadLocal<Long> lastTimestampInNanoseconds =
      new ThreadLocal<Long>() {
        @Override
        protected Long initialValue() {
          return System.nanoTime();
        }
      };

  public JavaRuntimeCircuitBreaker(@NonNull final JavaRuntimeCircuitBreakerConfig config) {
    super(config.getCircuitCheckIntervalInMillis());
    this.timeout =
        Optional.ofNullable(config.getTimeoutInSeconds()).map(s -> Duration.ofSeconds(s));
    this.config = config;
  }

  @Override
  public CircuitBreakerState currentState() {
    if (!isPercentageValueValid(config.getMemoryThreshold())
        || !isPercentageValueValid(config.getGcCpuThreshold())) {
      log.warn("Invalid memory threshold configured {}", config.getMemoryThreshold());
      return new CircuitBreakerState(false);
    }

    long currentTimeInNanoseconds = System.nanoTime();
    long gcCpuTime = getGCCpuTimeNS() - lastTotalGCTimeNS.get();
    long elapsedTime = currentTimeInNanoseconds - lastTimestampInNanoseconds.get();
    double gcCpuTimePercentage = (gcCpuTime / (double) elapsedTime) * 100;
    if (elapsedTime <= 0) {
      lastTimestampInNanoseconds.set(currentTimeInNanoseconds);
      lastTotalGCTimeNS.set(gcCpuTime);
      return new CircuitBreakerState(false);
    }
    double percentageFreeMemory =
        100 * ((freeMemory() + (maxMemory() - totalMemory())) / (double) maxMemory());

    lastTimestampInNanoseconds.set(currentTimeInNanoseconds);
    lastTotalGCTimeNS.set(lastTotalGCTimeNS.get() + gcCpuTime);

    int freeMemoryThreshold = config.getMemoryThreshold();
    int gcCPUThreshold = config.getGcCpuThreshold();

    String reason =
        String.format(
            "Circuit breaker tripped at memory %.2f%%  GC CPU time %.2f%% (freeMemoryThreshold %d%%, gcCPUThreshold %d%%)",
            percentageFreeMemory, gcCpuTimePercentage, freeMemoryThreshold, gcCPUThreshold);
    log.debug(reason);

    if (gcCpuTimePercentage >= gcCPUThreshold && percentageFreeMemory <= freeMemoryThreshold) {
      return new CircuitBreakerState(true, reason);
    }
    return new CircuitBreakerState(false);
  }

  /**
   * @return CPU time of old gen GC collection in nanoseconds
   */
  private long getGCCpuTimeNS() {
    return TimeUnit.NANOSECONDS.convert(
        getOldGenGCBean().getCollectionTime(), TimeUnit.MILLISECONDS);
  }

  private long getGCCount() {
    long gcCpuCount = 0;
    long collectorCount;
    for (GarbageCollectorMXBean gcBean : ManagementFactory.getGarbageCollectorMXBeans()) {
      collectorCount = gcBean.getCollectionCount();
      if (collectorCount != -1) {
        gcCpuCount += gcBean.getCollectionCount();
      }
    }
    return gcCpuCount;
  }

  /**
   * Returns old generation Garbage Collection bean. Refer to
   *
   * @see <a
   *     href"https://opensource.com/article/22/6/garbage-collection-java-virtual-machine-">this</a>
   *     to get more details on Java garbage collection. This method retrieves a list of {@link
   *     GarbageCollectorMXBean} and identifies the one corresponding to old generartion. This is
   *     just a bean with the least amount of gc counts with an exception of two corner case
   *     scenarios. First, in case of tie the amount of collection count is equal for young and old
   *     gen gc beans. Secondly, garbage collection can occur at the moment of running a method.
   *     This can be identified by comparing total amount of garbage collections at the beginning
   *     and at the end, while checking if a tie has occurred. In both scenarios above, we don't
   *     want to save found bean as cached.
   * @return GarbageCollector bean for the old (tenured) generation pool
   */
  private GarbageCollectorMXBean getOldGenGCBean() {
    if (null != this.oldGenGCBeanCached) {
      return this.oldGenGCBeanCached;
    }
    synchronized (this) {
      if (null != this.oldGenGCBeanCached) {
        return this.oldGenGCBeanCached;
      }

      GarbageCollectorMXBean lowestGCCountBean = null;
      log.debug("Circuit breaker: looking for old gen gc bean");

      boolean tie = false;
      long totalGCs = this.getGCCount();

      for (GarbageCollectorMXBean gcBean : getGarbageCollectorMXBeans()) {
        log.debug("Circuit breaker: checking {0}", gcBean.getName());
        if (null == lowestGCCountBean
            || lowestGCCountBean.getCollectionCount() > gcBean.getCollectionCount()) {
          tie = false;
          lowestGCCountBean = gcBean;
          continue;
        }
        if (lowestGCCountBean.getCollectionCount() == gcBean.getCollectionCount()) {
          tie = true;
        }
      }
      if (getGCCount() == totalGCs && !tie) {
        // gc hasn't happened in the middle of searching and there is a bean with the lowest count
        log.debug(
            "Circuit breaker: found and cached oldGenGCBean: {0}", lowestGCCountBean.getName());
        this.oldGenGCBeanCached = lowestGCCountBean;
        return oldGenGCBeanCached;
      } else {
        log.debug(
            "Circuit breaker: unable to find oldGenGCBean. Best guess: {0}",
            lowestGCCountBean.getName());
        return lowestGCCountBean;
      }
    }
  }
}
