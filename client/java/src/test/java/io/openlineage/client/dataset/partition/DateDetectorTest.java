/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.client.dataset.partition;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import org.junit.jupiter.api.Test;

class DateDetectorTest {

  @Test
  void testDateDetector() {
    assertThat(DateDetector.isDateMatch("20250721")).isTrue();
    assertThat(DateDetector.isDateMatch("20250722T0901Z")).isTrue();
    assertThat(DateDetector.isDateMatch("2025-07-22")).isTrue();
    assertThat(DateDetector.isDateMatch("20250722T901Z")).isTrue();

    assertThat(DateDetector.isDateMatch("20122321")).isFalse();
    assertThat(DateDetector.isDateMatch("dataset_20122321")).isFalse();
  }
}
