/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.utils;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class UUIDUtilsTest {

  @Test
  void testgenerateNewUUIDResultIsAlwaysDifferent() {
    assertThat(UUIDUtils.generateNewUUID().version()).isEqualTo(7);

    UUID uuid1 = UUIDUtils.generateNewUUID();
    UUID uuid2 = UUIDUtils.generateNewUUID();

    assertThat(uuid1).isNotEqualTo(uuid2);
  }

  @Test
  void testgenerateNewUUIDForInstantResultIsIncreasing() {
    Instant instant = Instant.now();
    assertThat(UUIDUtils.generateNewUUID(instant).version()).isEqualTo(7);

    UUID uuid1 = UUIDUtils.generateNewUUID(instant);
    UUID uuid2 = UUIDUtils.generateNewUUID(instant.plusMillis(1));

    assertThat(uuid1).isNotEqualTo(uuid2);
    assertThat(uuid1).isLessThan(uuid2);
  }
}
