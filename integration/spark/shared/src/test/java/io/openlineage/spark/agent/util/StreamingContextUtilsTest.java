/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.spark.agent.util;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class StreamingContextUtilsTest {
  @Test
  void verifyHasActiveStreamingContextWhenInactiveStreamingContext() {
    assertThat(StreamingContextUtils.hasActiveStreamingContext()).isFalse();
  }
}
