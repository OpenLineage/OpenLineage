/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import org.junit.jupiter.api.Test;

class GoogleCloudIntegrationTestDataTest {
  @Test
  void testConcurrentAttemptsUseIndependentPaths() {
    URI firstAttempt = GoogleCloudIntegrationTest.createRddTestBaseUri();
    URI secondAttempt = GoogleCloudIntegrationTest.createRddTestBaseUri();

    assertThat(firstAttempt.resolve("input/data.csv"))
        .isNotEqualTo(secondAttempt.resolve("input/data.csv"));
    assertThat(firstAttempt.resolve("output/data.csv"))
        .isNotEqualTo(secondAttempt.resolve("output/data.csv"));
    assertThat(firstAttempt.getScheme()).isEqualTo("gs");
    assertThat(secondAttempt.getAuthority()).isEqualTo(firstAttempt.getAuthority());
    assertThat(firstAttempt.getPath()).endsWith("/");
    assertThat(secondAttempt.getPath()).endsWith("/");
  }
}
