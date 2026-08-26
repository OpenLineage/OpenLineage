/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.spark.SparkContext;
import org.junit.jupiter.api.Test;

class ApplicationMetadataCacheTest {
  private static final String POSITIVE = "positive";
  private static final String NEGATIVE = "negative";
  private static final String VALUE_PREFIX = "value-";
  private static final String FIRST_VALUE = "value-1";

  @Test
  void cachesPositiveAndNegativeValuesPerSparkApplication() {
    SparkContext firstContext = mock(SparkContext.class);
    SparkContext secondContext = mock(SparkContext.class);
    AtomicInteger positiveLoads = new AtomicInteger();
    AtomicInteger negativeLoads = new AtomicInteger();

    ApplicationMetadataCache first = ApplicationMetadataCache.forSparkContext(firstContext);
    assertThat(first.get(POSITIVE, () -> VALUE_PREFIX + positiveLoads.incrementAndGet()))
        .isEqualTo(FIRST_VALUE);
    assertThat(first.get(POSITIVE, () -> VALUE_PREFIX + positiveLoads.incrementAndGet()))
        .isEqualTo(FIRST_VALUE);
    assertThat(first.get(NEGATIVE, () -> negativeValue(negativeLoads))).isEmpty();
    assertThat(first.get(NEGATIVE, () -> negativeValue(negativeLoads))).isEmpty();

    ApplicationMetadataCache second = ApplicationMetadataCache.forSparkContext(secondContext);
    assertThat(second.get(POSITIVE, () -> VALUE_PREFIX + positiveLoads.incrementAndGet()))
        .isEqualTo("value-2");

    assertThat(positiveLoads).hasValue(2);
    assertThat(negativeLoads).hasValue(1);
  }

  @Test
  void computesAValueOnceWhenReadersAreConcurrent() throws Exception {
    SparkContext sparkContext = mock(SparkContext.class);
    ApplicationMetadataCache cache = ApplicationMetadataCache.forSparkContext(sparkContext);
    ExecutorService executor = Executors.newFixedThreadPool(2);
    CountDownLatch loaderStarted = new CountDownLatch(1);
    CountDownLatch releaseLoader = new CountDownLatch(1);
    AtomicInteger loads = new AtomicInteger();

    try {
      Future<String> first =
          executor.submit(
              () ->
                  cache.get(
                      "concurrent",
                      () -> {
                        loads.incrementAndGet();
                        loaderStarted.countDown();
                        await(releaseLoader);
                        return "value";
                      }));
      loaderStarted.await();
      Future<String> second = executor.submit(() -> cache.get("concurrent", () -> "other"));
      releaseLoader.countDown();

      assertThat(first.get()).isEqualTo("value");
      assertThat(second.get()).isEqualTo("value");
      assertThat(loads).hasValue(1);
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  void expiresOnlyNegativeOptionalValues() {
    ApplicationMetadataCache cache = new ApplicationMetadataCache();
    AtomicInteger negativeLoads = new AtomicInteger();
    AtomicInteger positiveLoads = new AtomicInteger();

    assertThat(cache.getOptional(NEGATIVE, () -> negativeValue(negativeLoads), Duration.ZERO))
        .isEmpty();
    assertThat(cache.getOptional(NEGATIVE, () -> negativeValue(negativeLoads), Duration.ZERO))
        .isEmpty();
    assertThat(
            cache.getOptional(
                POSITIVE,
                () -> Optional.of(VALUE_PREFIX + positiveLoads.incrementAndGet()),
                Duration.ZERO))
        .contains(FIRST_VALUE);
    assertThat(
            cache.getOptional(
                POSITIVE,
                () -> Optional.of(VALUE_PREFIX + positiveLoads.incrementAndGet()),
                Duration.ZERO))
        .contains(FIRST_VALUE);

    assertThat(negativeLoads).hasValue(2);
    assertThat(positiveLoads).hasValue(1);
  }

  private static Optional<String> negativeValue(AtomicInteger loads) {
    loads.incrementAndGet();
    return Optional.empty();
  }

  private static void await(CountDownLatch latch) {
    try {
      latch.await();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
  }
}
