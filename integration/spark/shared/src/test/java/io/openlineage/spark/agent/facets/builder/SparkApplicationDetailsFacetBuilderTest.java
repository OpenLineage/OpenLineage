/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.facets.builder;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.Versions;
import io.openlineage.spark.agent.facets.SparkApplicationDetailsFacet;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.SparkOpenLineageConfig;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.scheduler.SparkListenerApplicationStart;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import scala.Option;

@SuppressWarnings("PMD.AvoidDuplicateLiterals")
class SparkApplicationDetailsFacetBuilderTest {
  private SparkContext sparkContext;
  private SparkConf sparkConf;
  private Configuration hadoopConf;
  private SparkApplicationDetailsFacetBuilder builder;

  @BeforeEach
  public void setupSparkContext() {
    sparkContext = mock(SparkContext.class);
    when(sparkContext.master()).thenReturn("local");
    when(sparkContext.appName()).thenReturn("someapp");
    when(sparkContext.applicationId()).thenReturn("app-123-456");
    when(sparkContext.sparkUser()).thenReturn("dummy");
    when(sparkContext.uiWebUrl()).thenReturn(Option.apply("http://driver.host:4040"));

    sparkConf =
        new SparkConf().set("spark.deploy.mode", "client").set("spark.driver.host", "some.host");
    when(sparkContext.getConf()).thenReturn(sparkConf);

    hadoopConf = new Configuration();
    when(sparkContext.hadoopConfiguration()).thenReturn(hadoopConf);

    builder =
        new SparkApplicationDetailsFacetBuilder(
            OpenLineageContext.builder()
                .sparkContext(sparkContext)
                .openLineage(new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI))
                .meterRegistry(new SimpleMeterRegistry())
                .openLineageConfig(new SparkOpenLineageConfig())
                .build());
  }

  @Test
  void testIsDefinedForSparkListenerApplicationStartEvent() {
    assertThat(builder.isDefinedAt(mock(SparkListenerApplicationStart.class))).isTrue();
  }

  @Test
  void testBuildNoUI() {
    // spark.ui.enabled=false
    when(sparkContext.uiWebUrl()).thenReturn(Option.apply(null));

    Map<String, OpenLineage.RunFacet> runFacetMap = new HashMap<>();
    builder.build(mock(SparkListenerApplicationStart.class), runFacetMap::put);

    assertThat(runFacetMap)
        .hasEntrySatisfying(
            "spark_applicationDetails",
            facet ->
                assertThat(facet)
                    .isInstanceOf(SparkApplicationDetailsFacet.class)
                    .hasFieldOrPropertyWithValue("master", "local")
                    .hasFieldOrPropertyWithValue("appName", "someapp")
                    .hasFieldOrPropertyWithValue("applicationId", "app-123-456")
                    .hasFieldOrPropertyWithValue("userName", "dummy")
                    .hasFieldOrPropertyWithValue("deployMode", "client")
                    .hasFieldOrPropertyWithValue("driverHost", "some.host")
                    .hasFieldOrPropertyWithValue("uiWebUrl", null)
                    .hasFieldOrPropertyWithValue("proxyUrl", null)
                    .hasFieldOrPropertyWithValue("historyUrl", null));
  }

  @Test
  void testBuildWithUI() {
    Map<String, OpenLineage.RunFacet> runFacetMap = new HashMap<>();
    builder.build(mock(SparkListenerApplicationStart.class), runFacetMap::put);
    assertThat(runFacetMap)
        .hasEntrySatisfying(
            "spark_applicationDetails",
            facet ->
                assertThat(facet)
                    .isInstanceOf(SparkApplicationDetailsFacet.class)
                    .hasFieldOrPropertyWithValue("master", "local")
                    .hasFieldOrPropertyWithValue("appName", "someapp")
                    .hasFieldOrPropertyWithValue("applicationId", "app-123-456")
                    .hasFieldOrPropertyWithValue("userName", "dummy")
                    .hasFieldOrPropertyWithValue("deployMode", "client")
                    .hasFieldOrPropertyWithValue("driverHost", "some.host")
                    .hasFieldOrPropertyWithValue("uiWebUrl", "http://driver.host:4040")
                    .hasFieldOrPropertyWithValue("proxyUrl", null)
                    .hasFieldOrPropertyWithValue("historyUrl", null));
  }

  @Test
  void testBuildWithYarnProxy() {
    sparkConf =
        sparkConf.set(
            "spark.org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter.param.PROXY_URI_BASES",
            "http://yarn.server:8088/proxy/app-123-456");

    Map<String, OpenLineage.RunFacet> runFacetMap = new HashMap<>();
    builder.build(mock(SparkListenerApplicationStart.class), runFacetMap::put);

    assertThat(runFacetMap)
        .hasEntrySatisfying(
            "spark_applicationDetails",
            facet ->
                assertThat(facet)
                    .isInstanceOf(SparkApplicationDetailsFacet.class)
                    .hasFieldOrPropertyWithValue("master", "local")
                    .hasFieldOrPropertyWithValue("appName", "someapp")
                    .hasFieldOrPropertyWithValue("applicationId", "app-123-456")
                    .hasFieldOrPropertyWithValue("userName", "dummy")
                    .hasFieldOrPropertyWithValue("deployMode", "client")
                    .hasFieldOrPropertyWithValue("driverHost", "some.host")
                    .hasFieldOrPropertyWithValue("uiWebUrl", "http://driver.host:4040")
                    .hasFieldOrPropertyWithValue(
                        "proxyUrl", "http://yarn.server:8088/proxy/app-123-456")
                    .hasFieldOrPropertyWithValue("historyUrl", null));
  }

  @Test
  void testBuildWithReverseProxyEnabled() {
    sparkConf =
        sparkConf
            .set("spark.ui.reverseProxy", "true")
            .set("spark.ui.reverseProxyUrl", "https://some.host/path/");

    Map<String, OpenLineage.RunFacet> runFacetMap = new HashMap<>();
    builder.build(mock(SparkListenerApplicationStart.class), runFacetMap::put);

    assertThat(runFacetMap)
        .hasEntrySatisfying(
            "spark_applicationDetails",
            facet ->
                assertThat(facet)
                    .isInstanceOf(SparkApplicationDetailsFacet.class)
                    .hasFieldOrPropertyWithValue("master", "local")
                    .hasFieldOrPropertyWithValue("appName", "someapp")
                    .hasFieldOrPropertyWithValue("applicationId", "app-123-456")
                    .hasFieldOrPropertyWithValue("userName", "dummy")
                    .hasFieldOrPropertyWithValue("deployMode", "client")
                    .hasFieldOrPropertyWithValue("driverHost", "some.host")
                    .hasFieldOrPropertyWithValue("uiWebUrl", "http://driver.host:4040")
                    .hasFieldOrPropertyWithValue(
                        "proxyUrl", "https://some.host/path/proxy/app-123-456")
                    .hasFieldOrPropertyWithValue("historyUrl", null));
  }

  @Test
  void testBuildWithReverseProxyDisabled() {
    sparkConf =
        sparkConf
            .set("spark.ui.reverseProxy", "false")
            .set("spark.ui.reverseProxyUrl", "https://some.host/path/");

    Map<String, OpenLineage.RunFacet> runFacetMap = new HashMap<>();
    builder.build(mock(SparkListenerApplicationStart.class), runFacetMap::put);

    assertThat(runFacetMap)
        .hasEntrySatisfying(
            "spark_applicationDetails",
            facet ->
                assertThat(facet)
                    .isInstanceOf(SparkApplicationDetailsFacet.class)
                    .hasFieldOrPropertyWithValue("master", "local")
                    .hasFieldOrPropertyWithValue("appName", "someapp")
                    .hasFieldOrPropertyWithValue("applicationId", "app-123-456")
                    .hasFieldOrPropertyWithValue("userName", "dummy")
                    .hasFieldOrPropertyWithValue("deployMode", "client")
                    .hasFieldOrPropertyWithValue("driverHost", "some.host")
                    .hasFieldOrPropertyWithValue("uiWebUrl", "http://driver.host:4040")
                    .hasFieldOrPropertyWithValue("proxyUrl", null)
                    .hasFieldOrPropertyWithValue("historyUrl", null));
  }

  @Test
  void testBuildWithHistoryServerHttp() {
    sparkConf = sparkConf.set("spark.yarn.historyServer.address", "history.server:18080");

    Map<String, OpenLineage.RunFacet> runFacetMap = new HashMap<>();
    builder.build(mock(SparkListenerApplicationStart.class), runFacetMap::put);

    assertThat(runFacetMap)
        .hasEntrySatisfying(
            "spark_applicationDetails",
            facet ->
                assertThat(facet)
                    .isInstanceOf(SparkApplicationDetailsFacet.class)
                    .hasFieldOrPropertyWithValue("master", "local")
                    .hasFieldOrPropertyWithValue("appName", "someapp")
                    .hasFieldOrPropertyWithValue("applicationId", "app-123-456")
                    .hasFieldOrPropertyWithValue("userName", "dummy")
                    .hasFieldOrPropertyWithValue("deployMode", "client")
                    .hasFieldOrPropertyWithValue("driverHost", "some.host")
                    .hasFieldOrPropertyWithValue("uiWebUrl", "http://driver.host:4040")
                    .hasFieldOrPropertyWithValue("proxyUrl", null)
                    .hasFieldOrPropertyWithValue(
                        "historyUrl", "http://history.server:18080/history/app-123-456"));
  }

  @Test
  void testBuildWithHistoryServerHttps() {
    sparkConf = sparkConf.set("spark.yarn.historyServer.address", "history.server:18080");
    hadoopConf.set("yarn.http.policy", "HTTPS_ONLY");

    Map<String, OpenLineage.RunFacet> runFacetMap = new HashMap<>();
    builder.build(mock(SparkListenerApplicationStart.class), runFacetMap::put);

    assertThat(runFacetMap)
        .hasEntrySatisfying(
            "spark_applicationDetails",
            facet ->
                assertThat(facet)
                    .isInstanceOf(SparkApplicationDetailsFacet.class)
                    .hasFieldOrPropertyWithValue("master", "local")
                    .hasFieldOrPropertyWithValue("appName", "someapp")
                    .hasFieldOrPropertyWithValue("applicationId", "app-123-456")
                    .hasFieldOrPropertyWithValue("userName", "dummy")
                    .hasFieldOrPropertyWithValue("deployMode", "client")
                    .hasFieldOrPropertyWithValue("driverHost", "some.host")
                    .hasFieldOrPropertyWithValue("uiWebUrl", "http://driver.host:4040")
                    .hasFieldOrPropertyWithValue("proxyUrl", null)
                    .hasFieldOrPropertyWithValue(
                        "historyUrl", "https://history.server:18080/history/app-123-456"));
  }

  @Test
  void testBuildWithHistoryServerWithSpecifiedUrl() {
    // for case then HistoryServer is served in K8s
    sparkConf =
        sparkConf.set("spark.yarn.historyServer.address", "https://history.server.domain/path");

    Map<String, OpenLineage.RunFacet> runFacetMap = new HashMap<>();
    builder.build(mock(SparkListenerApplicationStart.class), runFacetMap::put);

    assertThat(runFacetMap)
        .hasEntrySatisfying(
            "spark_applicationDetails",
            facet ->
                assertThat(facet)
                    .isInstanceOf(SparkApplicationDetailsFacet.class)
                    .hasFieldOrPropertyWithValue("master", "local")
                    .hasFieldOrPropertyWithValue("appName", "someapp")
                    .hasFieldOrPropertyWithValue("applicationId", "app-123-456")
                    .hasFieldOrPropertyWithValue("userName", "dummy")
                    .hasFieldOrPropertyWithValue("deployMode", "client")
                    .hasFieldOrPropertyWithValue("driverHost", "some.host")
                    .hasFieldOrPropertyWithValue("uiWebUrl", "http://driver.host:4040")
                    .hasFieldOrPropertyWithValue("proxyUrl", null)
                    .hasFieldOrPropertyWithValue(
                        "historyUrl", "https://history.server.domain/path/history/app-123-456"));
  }
}
