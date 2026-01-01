/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.client.dataset.partition.trimmer;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class YearMontTrimmerTest {

  YearMonthTrimmer trimmer = new YearMonthTrimmer();

  @Test
  void testTrim() {
    assertThat(trimmer.trim("/tmp/202507")).isEqualTo("/tmp");
    assertThat(trimmer.trim("/tmp/2025-07")).isEqualTo("/tmp");
    assertThat(trimmer.trim("tmp/2025-07")).isEqualTo("tmp");

    // this is not a valid year month
    assertThat(trimmer.trim("/tmp/202540")).isEqualTo("/tmp/202540");
  }

  @Test
  void testDoesNotTrimWholeName() {
    assertThat(trimmer.canTrim("202507")).isFalse();
    assertThat(trimmer.canTrim("/202507")).isFalse();
  }
}
