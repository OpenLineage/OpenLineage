/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.client.dataset.partition.trimmer;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class KeyValueTrimmerTest {

  KeyValueTrimmer trimmer = new KeyValueTrimmer();

  @Test
  void testTrim() {
    // nothing to normalize
    assertThat(trimmer.canTrim("/tmp/cat")).isFalse();

    // normalize key=value
    assertThat(trimmer.canTrim("/tmp/key=value")).isTrue();

    // don't normalize if more than one equality character detected
    assertThat(trimmer.canTrim("/tmp/a=b=c")).isFalse();
  }
}
