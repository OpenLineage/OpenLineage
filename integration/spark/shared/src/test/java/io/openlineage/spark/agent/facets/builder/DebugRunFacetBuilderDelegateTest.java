/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.facets.builder;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static scala.util.Properties.versionNumberString;

import io.openlineage.client.OpenLineageClientUtils;
import io.openlineage.client.transports.HttpConfig;
import io.openlineage.spark.agent.facets.DebugRunFacet.ClasspathDebugFacet;
import io.openlineage.spark.agent.facets.DebugRunFacet.LogicalPlanDebugFacet;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.SparkOpenLineageConfig;
import java.io.IOException;
import java.net.URISyntaxException;
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

@SuppressWarnings("PMD.AvoidDuplicateLiterals")
class DebugRunFacetBuilderDelegateTest {

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
  void testSystemDebugFacet() {
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
    when(openLineageContext.getSparkContext()).thenReturn(Optional.of(sparkContext));
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
  void testSparkConfigDebugFacet() throws URISyntaxException, IOException {
    SparkConf conf = new SparkConf();
    Catalog catalog = mock(Catalog.class);

    HttpConfig httpConfig = new HttpConfig();
    httpConfig.setEndpoint("http://localhost:5000");
    httpConfig.setAuth(() -> "api_key");
    SparkOpenLineageConfig config = new SparkOpenLineageConfig();
    config.setTransportConfig(httpConfig);

    when(openLineageContext.getSparkContext()).thenReturn(Optional.of(sparkContext));
    when(openLineageContext.getOpenLineageConfig()).thenReturn(config);
    when(sparkContext.conf()).thenReturn(conf);
    when(openLineageContext.getSparkSession()).thenReturn(Optional.of(session));
    when(session.catalog()).thenReturn(catalog);

    conf.set("spark.extraListeners", "listener1,listener2");
    conf.set("spark.sql.extensions", "ext1;ext2");

    assertThat(
            OpenLineageClientUtils.newObjectMapper()
                .writeValueAsString(delegate.buildFacet().getConfig()))
        .doesNotContain("api_key");

    assertThat(delegate.buildFacet().getConfig().getOpenLineageConfig()).isEqualTo(config);
    assertThat(delegate.buildFacet().getConfig())
        .hasFieldOrPropertyWithValue("extraListeners", "listener1,listener2")
        .hasFieldOrPropertyWithValue("extensions", "ext1;ext2")
        .hasFieldOrPropertyWithValue("catalogClass", catalog.getClass().getCanonicalName());
  }

  @Test
  void testBuildClasspathDebugFacet() {
    when(openLineageContext.getSparkContext()).thenReturn(Optional.of(sparkContext));
    when(sparkContext.listJars())
        .thenReturn(ScalaConversionUtils.fromList(Collections.singletonList("oneJar")));
    when(openLineageContext.getSparkVersion()).thenReturn("3.3.0");

    ClasspathDebugFacet facet = delegate.buildFacet().getClasspath();

    assertThat(facet.getJars()).containsExactly("oneJar");
    assertThat(facet)
        .hasFieldOrPropertyWithValue("sparkVersion", "3.3.0")
        .hasFieldOrPropertyWithValue("scalaVersion", versionNumberString());
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
