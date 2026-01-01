/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.client.dataset.partition.trimmer;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class DateTrimmerTest {

  DateTrimmer trimmer = new DateTrimmer();

  @Test
  void testTrim() {
    assertThat(trimmer.canTrim("/tmp/20250721")).isTrue();
    assertThat(trimmer.trim("/tmp/20250721")).isEqualTo("/tmp");
    assertThat(trimmer.trim("tmp/20250721")).isEqualTo("tmp");

    // non trivial pattern that should be considered date alike
    assertThat(trimmer.canTrim("/tmp/20250722T0901Z")).isTrue();
    assertThat(trimmer.canTrim("/tmp/2025-07-22")).isTrue();
    assertThat(trimmer.canTrim("/tmp/20250722T901Z")).isTrue();

    assertThat(trimmer.canTrim("/tmp/20250722T0901Z")).isTrue();
    assertThat(trimmer.canTrim("/tmp/2025-07-22")).isTrue();
    assertThat(trimmer.canTrim("/tmp/20250722T901Z")).isTrue();

    // non date alike patterns
    assertThat(trimmer.canTrim("/tmp/20122321")).isFalse();
    assertThat(trimmer.canTrim("/tmp/dataset_20122321")).isFalse();
  }

  @Test
  void testDoesNotTrimWholeName() {
    assertThat(trimmer.canTrim("20250721")).isFalse();
    assertThat(trimmer.canTrim("/20250721")).isFalse();
  }
}
