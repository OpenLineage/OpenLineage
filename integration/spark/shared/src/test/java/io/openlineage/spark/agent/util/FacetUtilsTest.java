/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import io.openlineage.client.transports.FacetsConfig;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.SparkOpenLineageConfig;
import org.junit.jupiter.api.Test;

class FacetUtilsTest {
  public static final String SPARK_LOGICAL_PLAN_FACET_NAME = "spark.logicalPlan";
  public static final String SPARK_UNKNOWN_FACET_NAME = "spark_unknown";
  public static final String DEBUG_FACET_NAME = "debug";

  @Test
  void testFacet() {
    OpenLineageContext olc = mock(OpenLineageContext.class);
    SparkOpenLineageConfig solc = mock(SparkOpenLineageConfig.class);
    when(olc.getOpenLineageConfig()).thenReturn(solc);
    FacetsConfig facetsConfig = new FacetsConfig();
    when(solc.getFacetsConfig()).thenReturn(facetsConfig);

    facetsConfig.setDisabledFacets(
        ImmutableMap.of(SPARK_LOGICAL_PLAN_FACET_NAME, false, SPARK_UNKNOWN_FACET_NAME, true));

    assertThat(FacetUtils.isFacetDisabled(olc, SPARK_LOGICAL_PLAN_FACET_NAME)).isFalse();
    assertThat(FacetUtils.isFacetDisabled(olc, SPARK_UNKNOWN_FACET_NAME)).isTrue();
  }

  @Test
  void testTheDefaults() {
    assertThat(FacetUtils.isFacetDisabled(null, "facetA")).isFalse();
    assertThat(FacetUtils.isFacetDisabled(null, SPARK_UNKNOWN_FACET_NAME)).isTrue();
    assertThat(FacetUtils.isFacetDisabled(null, SPARK_LOGICAL_PLAN_FACET_NAME)).isTrue();
    assertThat(FacetUtils.isFacetDisabled(null, DEBUG_FACET_NAME)).isTrue();
  }
}
