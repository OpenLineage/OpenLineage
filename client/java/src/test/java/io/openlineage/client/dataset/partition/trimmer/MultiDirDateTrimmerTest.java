/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.client.dataset.partition.trimmer;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class MultiDirDateTrimmerTest {

  MultiDirDateTrimmer trimmer = new MultiDirDateTrimmer();

  @Test
  void testTrim() {
    assertThat(trimmer.trim("/tmp")).isEqualTo("/tmp"); // nothing breaks
    assertThat(trimmer.trim("/a/b")).isEqualTo("/a/b"); // nothing breaks

    assertThat(trimmer.canTrim("/tmp/2025/07")).isTrue();
    assertThat(trimmer.trim("/tmp/2025/07")).isEqualTo("/tmp");

    assertThat(trimmer.canTrim("/tmp/2025/07/21")).isTrue();
    assertThat(trimmer.trim("/tmp/2025/07/21")).isEqualTo("/tmp");

    assertThat(trimmer.trim("/tmp/2025/14")).isEqualTo("/tmp/2025/14");
    assertThat(trimmer.trim("/tmp/2025/07/50")).isEqualTo("/tmp/2025/07/50");
  }
}
