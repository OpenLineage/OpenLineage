/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.spark.api.naming;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.SparkOpenLineageConfig;
import javax.annotation.Nullable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class OpenLineageAppNameApplicationJobNameProviderTest {
  OpenLineageAppNameApplicationJobNameProvider provider;

  private OpenLineageContext noAppNamePropertyOlContext;
  private OpenLineageContext appNamePropertySetOlContext1;
  private OpenLineageContext appNamePropertySetOlContext2;

  @BeforeEach
  void setUp() {
    provider = new OpenLineageAppNameApplicationJobNameProvider();
    // Case 1: The "spark.openlineage.appName" property is not set
    noAppNamePropertyOlContext = buildOpenLineageContextWithAppName(null);

    // Case 2: The "spark.openlineage.appName" property is set
    appNamePropertySetOlContext1 = buildOpenLineageContextWithAppName("Spark app name set by user");
    appNamePropertySetOlContext2 =
        buildOpenLineageContextWithAppName("Spark app name set by user cross-check");
  }

  @Test
  void testIsDefinedAt() {
    assertThat(provider.isDefinedAt(noAppNamePropertyOlContext)).isFalse();
    assertThat(provider.isDefinedAt(appNamePropertySetOlContext1)).isTrue();
    assertThat(provider.isDefinedAt(appNamePropertySetOlContext2)).isTrue();
  }

  @Test
  void testGetJobName() {
    assertThat(provider.getJobName(appNamePropertySetOlContext1))
        .isEqualTo("Spark app name set by user");
    assertThat(provider.getJobName(appNamePropertySetOlContext2))
        .isEqualTo("Spark app name set by user cross-check");
  }

  static OpenLineageContext buildOpenLineageContextWithAppName(
      @Nullable String openLineageAppNameProperty) {
    OpenLineageContext openLineageContext = mock(OpenLineageContext.class);
    SparkOpenLineageConfig sparkOpenLineageConfig = mock(SparkOpenLineageConfig.class);
    when(sparkOpenLineageConfig.getOverriddenAppName()).thenReturn(openLineageAppNameProperty);
    when(openLineageContext.getOpenLineageConfig()).thenReturn(sparkOpenLineageConfig);
    return openLineageContext;
  }
}
