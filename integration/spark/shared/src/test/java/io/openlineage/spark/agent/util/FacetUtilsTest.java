/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import io.openlineage.client.OpenLineage.RunFacetsBuilder;
import io.openlineage.client.transports.FacetsConfig;
import io.openlineage.spark.api.DebugConfig;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.SparkOpenLineageConfig;
import java.util.Optional;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
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

  @Test
  void testAttachSmartDebugFacetWhenDebugConfigIsNull() {
    OpenLineageContext context = mock(OpenLineageContext.class);
    SparkOpenLineageConfig config = mock(SparkOpenLineageConfig.class);
    when(context.getOpenLineageConfig()).thenReturn(config);
    RunFacetsBuilder runFacetsBuilder = mock(RunFacetsBuilder.class);

    FacetUtils.attachSmartDebugFacet(context, runFacetsBuilder, false, false);

    verify(runFacetsBuilder, never()).put(any(), any());
  }

  @Test
  void testAttachSmartDebugFacetWhenSmartDebugNotActive() {
    OpenLineageContext context = mock(OpenLineageContext.class);
    SparkOpenLineageConfig config = mock(SparkOpenLineageConfig.class);
    when(context.getOpenLineageConfig()).thenReturn(config);
    when(config.getDebugConfig()).thenReturn(new DebugConfig("enabled", "any-missing"));
    RunFacetsBuilder runFacetsBuilder = mock(RunFacetsBuilder.class);

    FacetUtils.attachSmartDebugFacet(context, runFacetsBuilder, false, false);

    verify(runFacetsBuilder, never()).put(any(), any());
  }

  @Test
  void testAttachSmartDebugFacetWhenSmartDebugActive() {
    SparkContext sparkContext = mock(SparkContext.class);
    SparkSession sparkSession = mock(SparkSession.class);
    OpenLineageContext context = mock(OpenLineageContext.class, RETURNS_DEEP_STUBS);
    SparkOpenLineageConfig config = mock(SparkOpenLineageConfig.class);
    when(context.getOpenLineageConfig()).thenReturn(config);
    when(context.getSparkContext()).thenReturn(Optional.of(sparkContext));
    when(sparkContext.getConf()).thenReturn(new SparkConf());
    when(context.getSparkSession()).thenReturn(Optional.of(sparkSession));
    when(sparkSession.catalog()).thenReturn(null);
    when(context.getLogicalPlan()).thenReturn(null);
    when(config.getDebugConfig()).thenReturn(new DebugConfig("enabled", "any-missing"));
    RunFacetsBuilder runFacetsBuilder = mock(RunFacetsBuilder.class);

    FacetUtils.attachSmartDebugFacet(context, runFacetsBuilder, true, false);

    verify(runFacetsBuilder, times(1)).put(any(), any());
  }
}
