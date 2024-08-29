/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.spark.api.naming;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.openlineage.spark.api.OpenLineageContext;
import java.util.Optional;
import javax.annotation.Nullable;
import org.apache.spark.SparkContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SparkApplicationNameApplicationJobNameProviderTest {

  private OpenLineageContext noSparkContextOlContext;
  private OpenLineageContext noSparkAppNameProperty;
  private OpenLineageContext blankSparkAppNameProperty;
  private OpenLineageContext specifiedSparkAppNameProperty1;
  private OpenLineageContext specifiedSparkAppNameProperty2;
  private SparkApplicationNameApplicationJobNameProvider provider;

  @BeforeEach
  void setUp() {
    provider = new SparkApplicationNameApplicationJobNameProvider();

    // Case 1: OpenLineage has no SparkContext set
    noSparkContextOlContext = mock(OpenLineageContext.class);
    when(noSparkContextOlContext.getSparkContext()).thenReturn(Optional.empty());

    // Case 2: SparkContext has no "spark.app.name" property set
    noSparkAppNameProperty = buildOpenLineageContextWithAppName(null);

    // Case 3: SparkContext has the property "spark.app.name" blank
    blankSparkAppNameProperty = buildOpenLineageContextWithAppName("   ");

    // Case 4: SparkContext has the property "spark.app.name" specified
    specifiedSparkAppNameProperty1 = buildOpenLineageContextWithAppName("Some Spark app");
    specifiedSparkAppNameProperty2 =
        buildOpenLineageContextWithAppName("Some Spark app cross-check");
  }

  @Test
  void testIsDefinedAt() {
    assertThat(provider.isDefinedAt(noSparkContextOlContext)).isTrue();
    assertThat(provider.isDefinedAt(noSparkAppNameProperty)).isTrue();
    assertThat(provider.isDefinedAt(blankSparkAppNameProperty)).isTrue();
    assertThat(provider.isDefinedAt(specifiedSparkAppNameProperty1)).isTrue();
    assertThat(provider.isDefinedAt(specifiedSparkAppNameProperty2)).isTrue();
  }

  @Test
  void testGetJobName() {
    assertThat(provider.getJobName(noSparkContextOlContext)).isEqualTo("unknown");
    assertThat(provider.getJobName(noSparkAppNameProperty)).isEqualTo("unknown");
    assertThat(provider.getJobName(blankSparkAppNameProperty)).isEqualTo("unknown");
    assertThat(provider.getJobName(specifiedSparkAppNameProperty1)).isEqualTo("Some Spark app");
    assertThat(provider.getJobName(specifiedSparkAppNameProperty2))
        .isEqualTo("Some Spark app cross-check");
  }

  static OpenLineageContext buildOpenLineageContextWithAppName(@Nullable String appName) {
    OpenLineageContext openLineageContext = mock(OpenLineageContext.class);
    SparkContext sparkContext = mock(SparkContext.class);
    when(sparkContext.appName()).thenReturn(appName);
    when(openLineageContext.getSparkContext()).thenReturn(Optional.of(sparkContext));
    return openLineageContext;
  }
}
