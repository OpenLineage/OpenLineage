/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.circuitBreaker;

import static io.openlineage.client.circuitBreaker.RuntimeUtils.freeMemory;
import static io.openlineage.client.circuitBreaker.RuntimeUtils.getGarbageCollectorMXBeans;
import static io.openlineage.client.circuitBreaker.RuntimeUtils.maxMemory;
import static io.openlineage.client.circuitBreaker.RuntimeUtils.totalMemory;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.util.concurrent.TimeUnit;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class JavaRuntimeCircuitBreaker extends CommonCircuitBreaker {
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
    this.config = config;
  }

  @Override
  public boolean isClosed() {
    if (!isPercentageValueValid(config.getMemoryThreshold())
        || !isPercentageValueValid(config.getGcCpuThreshold())) {
      log.warn("Invalid memory threshold configured {}", config.getMemoryThreshold());
      return false;
    }

    long currentTimeInNanoseconds = System.nanoTime();
    long gcCpuTime = getGCCpuTimeNS() - lastTotalGCTimeNS.get();
    long elapsedTime = currentTimeInNanoseconds - lastTimestampInNanoseconds.get();
    double gcCpuTimePercentage = (gcCpuTime / (double) elapsedTime) * 100;
    if (elapsedTime <= 0) {
      lastTimestampInNanoseconds.set(currentTimeInNanoseconds);
      lastTotalGCTimeNS.set(gcCpuTime);
      return false;
    }
    double percentageFreeMemory =
        100 * ((freeMemory() + (maxMemory() - totalMemory())) / (double) maxMemory());

    lastTimestampInNanoseconds.set(currentTimeInNanoseconds);
    lastTotalGCTimeNS.set(lastTotalGCTimeNS.get() + gcCpuTime);

    int freeMemoryThreshold = config.getMemoryThreshold();
    int gcCPUThreshold = config.getGcCpuThreshold();
    log.debug(
        "Circuit breaker: percentage free memory {}%  GC CPU time percentage {}% (freeMemoryThreshold {}, gcCPUThreshold {})",
        percentageFreeMemory, gcCpuTimePercentage, freeMemoryThreshold, gcCPUThreshold);

    if (gcCpuTimePercentage >= gcCPUThreshold && percentageFreeMemory <= freeMemoryThreshold) {
      log.warn(
          "Circuit breaker tripped at memory {}%  GC CPU time {}%",
          percentageFreeMemory, gcCpuTimePercentage);
      return true;
    }
    return false;
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
    long collectorCount = 0;
    for (GarbageCollectorMXBean gcBean : ManagementFactory.getGarbageCollectorMXBeans()) {
      collectorCount = gcBean.getCollectionCount();
      if (collectorCount != -1) {
        gcCpuCount += gcBean.getCollectionCount();
      }
    }
    return gcCpuCount;
  }

  /**
   * Bean with the least amount of gc counts == old gen GC Bean.
   *
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

  @Override
  public String getType() {
    return "javaRuntime";
  }
}
