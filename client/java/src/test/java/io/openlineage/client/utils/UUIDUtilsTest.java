/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.utils;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class UUIDUtilsTest {
  /** Extract timestamp from a UUIDv7. UUIDv7 has the timestamp in the first 48 bits */
  private long extractTimestampFromUuid(UUID uuid) {
    long msb = uuid.getMostSignificantBits();
    return (msb >>> 16) & 0xFFFFFFFFFFFL; // 48 bits
  }

  /** Extract random component from UUIDv7 for comparison */
  private long extractRandomFromUuid(UUID uuid) {
    return uuid.getLeastSignificantBits();
  }

  @Test
  void testGenerateNewUUIDUniqueness() {
    final int numUuids = 10000;
    Set<UUID> uuids = new HashSet<>();

    for (int i = 0; i < numUuids; i++) {
      UUID uuid = UUIDUtils.generateNewUUID();

      assertThat(uuids).doesNotContain(uuid);
      uuids.add(uuid);
    }
  }

  @Test
  void testGenerateNewUUIDUniquenessForSameInstant() {
    final int numUuids = 10000;
    Set<UUID> uuids = new HashSet<>();
    Instant instant = Instant.now();

    for (int i = 0; i < numUuids; i++) {
      UUID uuid = UUIDUtils.generateNewUUID(instant);

      assertThat(uuids).doesNotContain(uuid);
      uuids.add(uuid);
    }
  }

  @Test
  void testGenerateNewUUIDMonotonicIncrease() throws InterruptedException {
    final int numUuids = 100;

    UUID prevUuid = UUIDUtils.generateNewUUID();
    long prevTimestamp = extractTimestampFromUuid(prevUuid);

    // Sleep to ensure timestamp changes
    Thread.sleep(5);

    for (int i = 0; i < numUuids; i++) {
      UUID uuid = UUIDUtils.generateNewUUID();
      long timestamp = extractTimestampFromUuid(uuid);

      assertThat(timestamp).isGreaterThanOrEqualTo(prevTimestamp);
      prevTimestamp = timestamp;
    }
  }

  @Test
  void testGenerateNewUUIDTimestampCorrelation() {
    long before = Instant.now().toEpochMilli();
    UUID uuid = UUIDUtils.generateNewUUID();
    long after = Instant.now().toEpochMilli();

    long timestamp = extractTimestampFromUuid(uuid);
    assertThat(timestamp).isGreaterThanOrEqualTo(before).isLessThanOrEqualTo(after);
  }

  @Test
  void testGenerateNewUUIDPrefixDependsOnInstantMilliseconds() {
    Instant instantMilliseconds = Instant.parse("2025-05-20T10:52:33.881000Z");
    Instant instantMicroseconds = Instant.parse("2025-05-20T10:52:33.881863Z");

    UUID uuid1 = UUIDUtils.generateNewUUID(instantMilliseconds);
    UUID uuid2 = UUIDUtils.generateNewUUID(instantMicroseconds);

    assertThat(uuid1.toString()).matches(s -> s.startsWith("0196ed52-e0d9-7"));
    assertThat(uuid2.toString()).matches(s -> s.startsWith("0196ed52-e0d9-7"));
  }

  @Test
  void testGenerateNewUUIDRandomComponent() {
    final int numUuids = 1000;
    Set<Long> randomParts = new HashSet<>();

    for (int i = 0; i < numUuids; i++) {
      UUID uuid = UUIDUtils.generateNewUUID();
      Long randomPart = extractRandomFromUuid(uuid);

      assertThat(randomParts).doesNotContain(randomPart);
      randomParts.add(randomPart);
    }
  }

  @Test
  void testGenerateNewUUIDRandomComponentForSameInstant() {
    final int numUuids = 1000;
    Instant instant = Instant.now();
    Set<Long> randomParts = new HashSet<>();

    for (int i = 0; i < numUuids; i++) {
      UUID uuid = UUIDUtils.generateNewUUID(instant);
      Long randomPart = extractRandomFromUuid(uuid);

      assertThat(randomParts).doesNotContain(randomPart);
      randomParts.add(randomPart);
    }
  }

  @Test
  void testGenerateNewUUIDParallelGeneration() throws InterruptedException {
    final int numThreads = 8;
    final int uuidsPerThread = 1000;
    final Set<UUID> allUuids = Collections.newSetFromMap(new ConcurrentHashMap<>());
    final CountDownLatch latch = new CountDownLatch(numThreads);

    ExecutorService executor = Executors.newFixedThreadPool(numThreads);
    for (int t = 0; t < numThreads; t++) {
      executor.submit(
          () -> {
            try {
              for (int i = 0; i < uuidsPerThread; i++) {
                UUID uuid = UUIDUtils.generateNewUUID();

                assertThat(allUuids).doesNotContain(uuid);
                allUuids.add(uuid);
              }
            } finally {
              latch.countDown();
            }
          });
    }

    latch.await();
    executor.shutdown();
    executor.awaitTermination(10, TimeUnit.SECONDS);

    assertThat(allUuids).hasSize(numThreads * uuidsPerThread);
  }

  @Test
  void testGenerateNewUUIDParallelGenerationForSameInstant() throws InterruptedException {
    final int numThreads = 8;
    final int uuidsPerThread = 1000;
    Instant instant = Instant.now();
    final Set<UUID> allUuids = Collections.newSetFromMap(new ConcurrentHashMap<>());
    final CountDownLatch latch = new CountDownLatch(numThreads);

    ExecutorService executor = Executors.newFixedThreadPool(numThreads);
    for (int t = 0; t < numThreads; t++) {
      executor.submit(
          () -> {
            try {
              for (int i = 0; i < uuidsPerThread; i++) {
                UUID uuid = UUIDUtils.generateNewUUID(instant);

                assertThat(allUuids).doesNotContain(uuid);
                allUuids.add(uuid);
              }
            } finally {
              latch.countDown();
            }
          });
    }

    latch.await();
    executor.shutdown();
    executor.awaitTermination(10, TimeUnit.SECONDS);

    assertThat(allUuids).hasSize(numThreads * uuidsPerThread);
  }

  @Test
  void testGenerateNewUUIDVersion() {
    UUID uuid = UUIDUtils.generateNewUUID();

    assertThat(uuid.version()).isEqualTo(7);
    assertThat(uuid.variant()).isEqualTo(2);
  }

  @Test
  void testGenerateStaticUUIDReturnsSameResultForSameInput() {
    Instant instant = Instant.now();
    byte[] data = "some".getBytes(UTF_8);

    UUID uuid1 = UUIDUtils.generateStaticUUID(instant, data);
    UUID uuid2 = UUIDUtils.generateStaticUUID(instant, data);

    assertThat(uuid1.version()).isEqualTo(7);
    assertThat(uuid2.version()).isEqualTo(7);
    assertThat(uuid1).isEqualTo(uuid2);
  }

  @ParameterizedTest
  @CsvSource({"some,some", "some,other"})
  void testGenerateStaticUUIDResultIsIncreasingWithInstantIncrement(String data1, String data2) {
    Instant instant1 = Instant.now();
    Instant instant2 = instant1.plusMillis(1);

    UUID uuid1 = UUIDUtils.generateStaticUUID(instant1, data1.getBytes(UTF_8));
    UUID uuid2 = UUIDUtils.generateStaticUUID(instant2, data2.getBytes(UTF_8));

    assertThat(uuid1.version()).isEqualTo(7);
    assertThat(uuid2.version()).isEqualTo(7);
    assertThat(uuid1).isNotEqualTo(uuid2);
    assertThat(uuid1).isLessThan(uuid2);

    // both timestamp and random parts are different
    assertThat(uuid1.getMostSignificantBits()).isNotEqualTo(uuid2.getMostSignificantBits());
    assertThat(uuid1.getLeastSignificantBits()).isNotEqualTo(uuid2.getLeastSignificantBits());
  }

  @ParameterizedTest
  @CsvSource({"some", "other"})
  void testGenerateStaticUUIDResultPrefixDependsOnInstantMilliseconds(String data) {
    Instant instantMilliseconds = Instant.parse("2025-05-20T10:52:33.881000Z");
    Instant instantMicroseconds = Instant.parse("2025-05-20T10:52:33.881863Z");

    UUID uuid1 = UUIDUtils.generateStaticUUID(instantMilliseconds, data.getBytes(UTF_8));
    UUID uuid2 = UUIDUtils.generateStaticUUID(instantMicroseconds, data.getBytes(UTF_8));

    assertThat(uuid1.toString()).matches(s -> s.startsWith("0196ed52-e0d9-7"));
    assertThat(uuid2.toString()).matches(s -> s.startsWith("0196ed52-e0d9-7"));
  }

  @Test
  void testGenerateStaticUUIDResultIsDifferentForDifferentData() {
    Instant instant = Instant.now();

    UUID uuid1 = UUIDUtils.generateStaticUUID(instant, "some".getBytes(UTF_8));
    UUID uuid2 = UUIDUtils.generateStaticUUID(instant, "other".getBytes(UTF_8));

    assertThat(uuid1.version()).isEqualTo(7);
    assertThat(uuid2.version()).isEqualTo(7);
    assertThat(uuid1).isNotEqualTo(uuid2);

    // timestamp part is the same
    assertThat(uuid1.getMostSignificantBits() & 0x0000L)
        .isEqualTo(uuid2.getMostSignificantBits() & 0x0000L);

    // random part is different
    assertThat(uuid1.getMostSignificantBits() & 0xffffL)
        .isNotEqualTo(uuid2.getMostSignificantBits() & 0xffffL);
    assertThat(uuid1.getLeastSignificantBits()).isNotEqualTo(uuid2.getLeastSignificantBits());
  }
}
