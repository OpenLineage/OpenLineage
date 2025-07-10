/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.spark.api.naming;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.openlineage.spark.api.OpenLineageContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SparkApplicationNameApplicationJobNameProviderTest {

  private OpenLineageContext withAppName;
  private SparkApplicationNameApplicationJobNameProvider provider;

  @BeforeEach
  void setUp() {
    provider = new SparkApplicationNameApplicationJobNameProvider();

    // Case 2: SparkContext has no "spark.app.name" property set
    withAppName = buildOpenLineageContextWithAppName();
  }

  @Test
  void testIsDefinedAt() {
    assertThat(provider.isDefinedAt(withAppName)).isTrue();
  }

  @Test
  void testGetJobName() {
    assertThat(provider.getJobName(withAppName)).isEqualTo("appNameFromStartEvent");
  }

  static OpenLineageContext buildOpenLineageContextWithAppName() {
    OpenLineageContext openLineageContext = mock(OpenLineageContext.class);
    when(openLineageContext.getApplicationName()).thenReturn("appNameFromStartEvent");
    return openLineageContext;
  }
}
