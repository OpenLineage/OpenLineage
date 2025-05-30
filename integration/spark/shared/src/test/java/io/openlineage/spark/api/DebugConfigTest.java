/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.spark.api;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class DebugConfigTest {

  DebugConfig debugConfig = new DebugConfig();

  @Test
  void testDebugConfigDisabled() {
    debugConfig.setSmartDebugEnabled("disabled");
    assertThat(debugConfig.isSmartDebugActive(false, false)).isFalse();
  }

  @Test
  void testDebugConfigEnabledForAnyMissing() {
    debugConfig.setSmartDebugEnabled("ENABLEd");
    debugConfig.setMode("any-missing");

    // only input missing
    assertThat(debugConfig.isSmartDebugActive(true, false)).isTrue();

    // only output missing
    assertThat(debugConfig.isSmartDebugActive(false, true)).isTrue();
  }

  @Test
  void testDebugConfigEnabledForOutputMissing() {
    debugConfig.setSmartDebugEnabled("enabled");
    debugConfig.setMode("output-missing");

    // only input missing
    assertThat(debugConfig.isSmartDebugActive(true, false)).isFalse();

    // only output missing
    assertThat(debugConfig.isSmartDebugActive(false, true)).isTrue();
  }

  @Test
  void testUnsupportedMode() {
    debugConfig.setSmartDebugEnabled("enabled");
    debugConfig.setMode("unsupported-mode");

    assertThat(debugConfig.isSmartDebugActive(true, true)).isFalse();
  }
}
