/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.util;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.function.Supplier;
import java.util.function.ToLongFunction;
import lombok.SneakyThrows;
import org.apache.spark.SparkContext;

/**
 * Stores metadata that is stable for the lifetime of one Spark application.
 *
 * <p>The registry is keyed by {@link SparkContext}, rather than globally by cloud provider, so
 * sequential Spark applications in the same JVM cannot reuse one another's cloud identity. Weak
 * keys allow stopped contexts to be collected; cached values must therefore not retain the Spark
 * context itself.
 */
public final class ApplicationMetadataCache {

  private static final Map<SparkContext, ApplicationMetadataCache> APPLICATION_CACHES =
      new WeakHashMap<>();

  private final Map<String, CacheEntry> values = new ConcurrentHashMap<>();

  public static ApplicationMetadataCache forSparkContext(SparkContext sparkContext) {
    Objects.requireNonNull(sparkContext, "sparkContext");
    synchronized (APPLICATION_CACHES) {
      return APPLICATION_CACHES.computeIfAbsent(
          sparkContext, ignored -> new ApplicationMetadataCache());
    }
  }

  /** Removes an application's cached metadata immediately, primarily for deterministic teardown. */
  public static void invalidate(SparkContext sparkContext) {
    synchronized (APPLICATION_CACHES) {
      APPLICATION_CACHES.remove(sparkContext);
    }
  }

  /**
   * Returns the cached value or computes it once. Values such as {@code Optional.empty()} are
   * cached, allowing callers to represent a definitive negative lookup without retrying it for
   * every event.
   */
  @SuppressWarnings("unchecked")
  @SneakyThrows
  public <T> T get(String key, Supplier<T> loader) {
    return get(key, loader, value -> Long.MAX_VALUE);
  }

  /**
   * Caches a present value for the application lifetime and an unavailable value for a bounded
   * interval. This avoids retrying transient cloud-service failures for every event without making
   * a temporary failure permanent.
   */
  public <T> Optional<T> getOptional(
      String key, Supplier<Optional<T>> loader, Duration negativeTtl) {
    Objects.requireNonNull(negativeTtl, "negativeTtl");
    return get(
        key,
        loader,
        value ->
            ((Optional<?>) value).isPresent()
                ? Long.MAX_VALUE
                : System.nanoTime() + Math.max(0L, negativeTtl.toNanos()));
  }

  @SuppressWarnings("unchecked")
  @SneakyThrows
  private <T> T get(String key, Supplier<T> loader, ToLongFunction<Object> expiration) {
    Objects.requireNonNull(key, "key");
    Objects.requireNonNull(loader, "loader");

    CacheEntry entry;
    boolean retry;
    do {
      retry = false;
      entry = values.get(key);
      if (entry != null && entry.isExpired()) {
        values.remove(key, entry);
        retry = true;
      } else if (entry == null) {
        CacheEntry newEntry = new CacheEntry(loader, expiration);
        entry = values.putIfAbsent(key, newEntry);
        if (entry == null) {
          entry = newEntry;
          newEntry.run();
        } else if (entry.isExpired()) {
          retry = true;
        }
      }
    } while (retry);

    try {
      return (T) entry.get();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw e;
    } catch (ExecutionException e) {
      values.remove(key, entry);
      throw e.getCause();
    }
  }

  private static final class CacheEntry {
    private final FutureTask<Object> future;
    private volatile long expiresAt = Long.MAX_VALUE;

    private <T> CacheEntry(Supplier<T> loader, ToLongFunction<Object> expiration) {
      future =
          new FutureTask<>(
              () -> {
                T value = loader.get();
                expiresAt = expiration.applyAsLong(value);
                return value;
              });
    }

    private void run() {
      future.run();
    }

    private Object get() throws InterruptedException, ExecutionException {
      return future.get();
    }

    private boolean isExpired() {
      return future.isDone() && System.nanoTime() >= expiresAt;
    }
  }
}
