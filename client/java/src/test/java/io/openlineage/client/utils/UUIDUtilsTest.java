/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.utils;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class UUIDUtilsTest {

  @Test
  void testGenerateNewUUIDResultIsAlwaysDifferent() {
    UUID uuid1 = UUIDUtils.generateNewUUID();
    UUID uuid2 = UUIDUtils.generateNewUUID();

    assertThat(uuid1.version()).isEqualTo(7);
    assertThat(uuid2.version()).isEqualTo(7);
    assertThat(uuid1).isNotEqualTo(uuid2);

    Instant instant = Instant.now();
    uuid1 = UUIDUtils.generateNewUUID(instant);
    uuid2 = UUIDUtils.generateNewUUID(instant);

    assertThat(uuid1.version()).isEqualTo(7);
    assertThat(uuid2.version()).isEqualTo(7);
    assertThat(uuid1).isNotEqualTo(uuid2);
  }

  @Test
  void testGenerateNewUUIDForInstantResultIsIncreasing() {
    Instant instant1 = Instant.now();
    Instant instant2 = instant1.plusMillis(1);

    UUID uuid1 = UUIDUtils.generateNewUUID(instant1);
    UUID uuid2 = UUIDUtils.generateNewUUID(instant2);

    assertThat(uuid1.version()).isEqualTo(7);
    assertThat(uuid2.version()).isEqualTo(7);
    assertThat(uuid1).isNotEqualTo(uuid2);
    assertThat(uuid1).isLessThan(uuid2);
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
