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
    debugConfig.setSmartEnabled(false);
    assertThat(debugConfig.isSmartDebugActive(false, false)).isFalse();
  }

  @Test
  void testDebugConfigEnabledForAnyMissing() {
    debugConfig.setSmartEnabled(true);
    debugConfig.setMode("any-missing");

    // only input missing
    assertThat(debugConfig.isSmartDebugActive(true, false)).isTrue();

    // only output missing
    assertThat(debugConfig.isSmartDebugActive(false, true)).isTrue();

    assertThat(debugConfig.isSmartDebugActive(true, true)).isFalse();
  }

  @Test
  void testDebugConfigEnabledForOutputMissing() {
    debugConfig.setSmartEnabled(true);
    debugConfig.setMode("output-missing");

    // no output
    assertThat(debugConfig.isSmartDebugActive(true, false)).isTrue();

    // output detected
    assertThat(debugConfig.isSmartDebugActive(false, true)).isFalse();
    assertThat(debugConfig.isSmartDebugActive(true, true)).isFalse();
  }

  @Test
  void testMissingOrUnsupportedSmartMode() {
    debugConfig.setSmartEnabled(true);

    // no mode set, should default to false
    debugConfig.setMode(null);
    assertThat(debugConfig.isSmartDebugActive(false, true)).isTrue();

    // unsupported mode
    debugConfig.setMode("unsupported-mode");
    assertThat(debugConfig.isSmartDebugActive(false, true)).isTrue();
  }

  @Test
  void testMetricsDisabled() {
    assertThat(debugConfig.isMetricsDisabled()).isFalse();

    debugConfig.setMetricsDisabled(true);
    assertThat(debugConfig.isMetricsDisabled()).isTrue();
  }
}
