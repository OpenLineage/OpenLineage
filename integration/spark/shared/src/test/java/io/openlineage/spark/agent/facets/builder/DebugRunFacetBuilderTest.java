/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.facets.builder;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.SparkOpenLineageConfig;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class DebugRunFacetBuilderTest {

  private static OpenLineageContext openLineageContext =
      mock(OpenLineageContext.class, RETURNS_DEEP_STUBS);
  private static DebugRunFacetBuilder builder = new DebugRunFacetBuilder(openLineageContext);
  private static SparkOpenLineageConfig config = new SparkOpenLineageConfig();

  @BeforeAll
  static void setup() {
    builder = new DebugRunFacetBuilder(openLineageContext);
    when(openLineageContext.getOpenLineageConfig()).thenReturn(config);
  }

  @Test
  void testIsDefinedAtWhenDebugEnabled() {
    config.getFacetsConfig().getDisabledFacets().put("debug", false);
    assertThat(builder.isDefinedAt(mock(Object.class))).isTrue();
  }

  @Test
  void testIsDefinedAtWhenDebugDisabled() {
    config.getFacetsConfig().getDisabledFacets().put("debug", true);
    assertThat(builder.isDefinedAt(mock(Object.class))).isFalse();
  }
}
