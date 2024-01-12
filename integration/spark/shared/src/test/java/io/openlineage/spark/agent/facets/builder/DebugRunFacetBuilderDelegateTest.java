/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.facets.builder;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.openlineage.spark.agent.facets.DebugRunFacet.ClasspathDebugFacet;
import io.openlineage.spark.agent.facets.DebugRunFacet.LogicalPlanDebugFacet;
import io.openlineage.spark.agent.facets.DebugRunFacet.SparkConfigDebugFacet;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.api.OpenLineageContext;
import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;
import org.apache.commons.lang3.SystemUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalog.Catalog;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.QueryExecution;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class DebugRunFacetBuilderDelegateTest {

  private static OpenLineageContext openLineageContext =
      mock(OpenLineageContext.class, RETURNS_DEEP_STUBS);
  private static SparkContext sparkContext = mock(SparkContext.class);
  private static DebugRunFacetBuilderDelegate delegate;
  private static SparkSession session = mock(SparkSession.class);
  private static QueryExecution queryExecution = mock(QueryExecution.class);

  @BeforeAll
  static void setup() {
    delegate = new DebugRunFacetBuilderDelegate(openLineageContext);
    when(openLineageContext.getSparkSession()).thenReturn(Optional.of(session));
    when(openLineageContext.getQueryExecution()).thenReturn(Optional.of(queryExecution));
  }

  @Test
  public void testSystemDebugFacet() {
    assertThat(delegate.buildFacet().getSystem())
        .hasFieldOrPropertyWithValue("javaVersion", SystemUtils.JAVA_VERSION)
        .hasFieldOrPropertyWithValue("javaVendor", SystemUtils.JAVA_VENDOR)
        .hasFieldOrPropertyWithValue("osArch", SystemUtils.OS_ARCH)
        .hasFieldOrPropertyWithValue("osName", SystemUtils.OS_NAME)
        .hasFieldOrPropertyWithValue("osVersion", SystemUtils.OS_VERSION)
        .hasFieldOrPropertyWithValue("userLanguage", SystemUtils.USER_LANGUAGE)
        .hasFieldOrPropertyWithValue("userTimezone", SystemUtils.USER_TIMEZONE)
        .hasFieldOrPropertyWithValue("sparkDeployMode", null);
  }

  @Test
  void testSystemDebugFacetExecutionContext() {
    when(openLineageContext.getSparkContext()).thenReturn(sparkContext);
    when(sparkContext.deployMode()).thenReturn("local");
    assertThat(delegate.buildFacet().getSystem())
        .hasFieldOrPropertyWithValue("sparkDeployMode", "local");
  }

  @Test
  void testSparkConfigDebugFacetWhenSparkConfIsEmpty() {
    SparkConf conf = mock(SparkConf.class);
    when(sparkContext.getConf()).thenReturn(conf);
    assertThat(delegate.buildFacet().getConfig().getCatalogClass()).isNull();
  }

  @Test
  void testSparkConfigDebugFacet() {
    SparkConf conf = new SparkConf();
    Catalog catalog = mock(Catalog.class);

    when(openLineageContext.getSparkContext()).thenReturn(sparkContext);
    when(sparkContext.conf()).thenReturn(conf);
    when(openLineageContext.getSparkSession()).thenReturn(Optional.of(session));
    when(session.catalog()).thenReturn(catalog);

    conf.set("spark.extraListeners", "listener1,listener2");
    conf.set("spark.sql.extensions", "ext1;ext2");

    conf.set("spark.openlineage.transport.type", "http");
    conf.set("spark.openlineage.transport.url", "http://localhost:5000");
    conf.set("spark.openlineage.transport.auth.apiKey", "key"); // should be skipped
    conf.set("spark.nonopenlineage", "value"); // should be skipped

    SparkConfigDebugFacet configDebugFacet = delegate.buildFacet().getConfig();
    assertThat(configDebugFacet)
        .hasFieldOrPropertyWithValue("extraListeners", "listener1,listener2")
        .hasFieldOrPropertyWithValue("extensions", "ext1;ext2")
        .hasFieldOrPropertyWithValue("catalogClass", catalog.getClass().getCanonicalName());

    assertThat(configDebugFacet.getOpenLineageConfig())
        .doesNotContainKey("spark.nonopenlineage")
        .doesNotContainKey("spark.openlineage.transport.auth.apiKey")
        .containsEntry("transport.type", "http")
        .containsEntry("transport.url", "http://localhost:5000");
  }

  @Test
  void testBuildClasspathDebugFacet() {
    when(openLineageContext.getSparkContext()).thenReturn(sparkContext);
    when(sparkContext.listJars())
        .thenReturn(ScalaConversionUtils.fromList(Collections.singletonList("oneJar")));
    when(openLineageContext.getSparkVersion()).thenReturn("3.3.0");

    ClasspathDebugFacet facet = delegate.buildFacet().getClasspath();

    assertThat(facet.getJars()).containsExactly("oneJar");
    assertThat(facet)
        .hasFieldOrPropertyWithValue("sparkVersion", "3.3.0")
        .hasFieldOrPropertyWithValue("scalaVersion", "2.11.12");
  }

  @Test
  void testBuildLogicalPlanDebugFacet() {
    LogicalPlan root = mock(LogicalPlan.class);
    LogicalPlan node = mock(LogicalPlan.class);
    LogicalPlan leaf1 = mock(LogicalPlan.class);
    LogicalPlan leaf2 = mock(LogicalPlan.class);

    when(root.nodeName()).thenReturn("CreateTable");
    when(root.toString()).thenReturn("CreateTable-toString");
    when(node.nodeName()).thenReturn("Node");
    when(leaf1.nodeName()).thenReturn("LogicalRelation");
    when(leaf2.nodeName()).thenReturn("LogicalRelation");

    when(queryExecution.optimizedPlan()).thenReturn(root);
    when(root.children())
        .thenReturn(ScalaConversionUtils.fromList(Collections.singletonList(node)));
    when(node.children()).thenReturn(ScalaConversionUtils.fromList(Arrays.asList(leaf1, leaf2)));

    LogicalPlanDebugFacet facet = delegate.buildFacet().getLogicalPlan();

    assertThat(facet.getNodes()).hasSize(4);
    assertThat(facet.getNodes().get(0))
        .hasFieldOrPropertyWithValue("id", "CreateTable@" + root.hashCode())
        .hasFieldOrPropertyWithValue("children", Arrays.asList("Node@" + node.hashCode()))
        .hasFieldOrPropertyWithValue("desc", "CreateTable-toString");

    assertThat(facet.getNodes().get(1))
        .hasFieldOrPropertyWithValue("id", "Node@" + node.hashCode())
        .hasFieldOrPropertyWithValue(
            "children",
            Arrays.asList(
                "LogicalRelation@" + leaf1.hashCode(), "LogicalRelation@" + leaf2.hashCode()));

    assertThat(facet.getNodes().get(2))
        .hasFieldOrPropertyWithValue("id", "LogicalRelation@" + leaf1.hashCode())
        .hasFieldOrPropertyWithValue("children", null);

    assertThat(facet.getNodes().get(3))
        .hasFieldOrPropertyWithValue("id", "LogicalRelation@" + leaf2.hashCode())
        .hasFieldOrPropertyWithValue("children", null);
  }
}
