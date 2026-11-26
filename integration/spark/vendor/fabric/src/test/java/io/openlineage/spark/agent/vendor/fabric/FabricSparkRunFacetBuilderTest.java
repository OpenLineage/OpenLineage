/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.vendor.fabric;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.Versions;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.SparkOpenLineageConfig;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.scheduler.SparkListenerApplicationStart;
import org.apache.spark.scheduler.SparkListenerJobStart;
import org.junit.jupiter.api.Test;

@SuppressWarnings("PMD.AvoidDuplicateLiterals")
class FabricSparkRunFacetBuilderTest {
  // Values from Fabric Spark sessions (a notebook run by a pipeline).
  private static final String WORKSPACE = "8b4cb800-a044-43ce-8f76-3356f709b468";
  private static final String NOTEBOOK = "af3e0877-abde-4457-8e7d-2f6026993a02";
  private static final String LIVY = "fa3ea1fc-c6bf-4b1d-a926-9be1bbf4851c";
  private static final String JOB_INSTANCE = "a266f2f5-8c88-4788-8915-cb4d4456b790";
  private static final String JOB_IDENTIFIER =
      "22dynamicdatacloudworkspace.datacloudpool1-4353128699960801747." + LIVY;

  private static final OpenLineage OL = new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI);

  private static Map<String, String> fabricConf() {
    Map<String, String> conf = new HashMap<>();
    conf.put(FabricSparkRunFacetBuilder.JOB_IDENTIFIER_CONF, JOB_IDENTIFIER);
    conf.put(FabricSparkRunFacetBuilder.WORKSPACE_ID_CONF, WORKSPACE);
    conf.put(FabricSparkRunFacetBuilder.ITEM_ID_CONF, NOTEBOOK);
    conf.put(FabricSparkRunFacetBuilder.ITEM_TYPE_CONF, "SynapseNotebook");
    conf.put(FabricSparkRunFacetBuilder.JOB_INSTANCE_ID_CONF, JOB_INSTANCE);
    return conf;
  }

  private static Optional<OpenLineage.FabricSparkRunFacet> facetFor(Map<String, String> conf) {
    return FabricSparkRunFacetBuilder.facetFor(OL, key -> Optional.ofNullable(conf.get(key)));
  }

  @Test
  void buildsTheFacetFromTheSessionSettings() {
    OpenLineage.FabricSparkRunFacet facet = facetFor(fabricConf()).get();
    assertThat(facet.getWorkspaceId()).isEqualTo(WORKSPACE);
    assertThat(facet.getItemId()).isEqualTo(NOTEBOOK);
    assertThat(facet.getItemType()).isEqualTo("SynapseNotebook");
    assertThat(facet.getLivyId()).isEqualTo(LIVY);
    assertThat(facet.getJobInstanceId()).isEqualTo(JOB_INSTANCE);
  }

  @Test
  void lowercasesIds() {
    Map<String, String> conf = fabricConf();
    conf.put(FabricSparkRunFacetBuilder.WORKSPACE_ID_CONF, WORKSPACE.toUpperCase(Locale.ROOT));
    conf.put(
        FabricSparkRunFacetBuilder.JOB_IDENTIFIER_CONF, "1x.pool." + LIVY.toUpperCase(Locale.ROOT));
    OpenLineage.FabricSparkRunFacet facet = facetFor(conf).get();
    assertThat(facet.getWorkspaceId()).isEqualTo(WORKSPACE);
    assertThat(facet.getLivyId()).isEqualTo(LIVY);
  }

  @Test
  void dropsValuesThatAreNotIds() {
    Map<String, String> conf = fabricConf();
    conf.put(FabricSparkRunFacetBuilder.ITEM_ID_CONF, "notebook");
    conf.put(FabricSparkRunFacetBuilder.JOB_IDENTIFIER_CONF, "22dynamicdatacloudworkspace.pool");
    OpenLineage.FabricSparkRunFacet facet = facetFor(conf).get();
    assertThat(facet.getItemId()).isNull();
    assertThat(facet.getLivyId()).isNull();
    assertThat(facet.getWorkspaceId()).isEqualTo(WORKSPACE);
  }

  @Test
  void livyIdIsTheLastSegmentOfTheJobIdentifier() {
    assertThat(FabricSparkRunFacetBuilder.livyId(JOB_IDENTIFIER)).contains(LIVY);
    assertThat(FabricSparkRunFacetBuilder.livyId(LIVY)).contains(LIVY);
    assertThat(FabricSparkRunFacetBuilder.livyId("a.b.")).isEmpty();
    assertThat(FabricSparkRunFacetBuilder.livyId("")).isEmpty();
  }

  @Test
  void noFacetOutsideFabric() {
    Map<String, String> conf = new HashMap<>();
    conf.put("spark.app.name", "etl");
    assertThat(facetFor(conf)).isEmpty();
  }

  @Test
  void addsTheFacetToEveryEvent() {
    SparkConf conf = new SparkConf();
    fabricConf().forEach(conf::set);
    FabricSparkRunFacetBuilder builder = builderFor(conf);
    for (Object event :
        new Object[] {
          mock(SparkListenerApplicationStart.class), mock(SparkListenerJobStart.class)
        }) {
      Map<String, Object> facets = new HashMap<>();
      builder.accept(event, facets::put);
      assertThat(facets).containsKey("fabric_spark");
      assertThat(((OpenLineage.FabricSparkRunFacet) facets.get("fabric_spark")).getLivyId())
          .isEqualTo(LIVY);
    }
  }

  @Test
  void unreadableSettingsGiveNoFacet() {
    SparkContext sparkContext = mock(SparkContext.class);
    when(sparkContext.getConf()).thenThrow(new IllegalStateException("stopped"));
    FabricSparkRunFacetBuilder builder = new FabricSparkRunFacetBuilder(context(sparkContext));
    Map<String, Object> facets = new HashMap<>();
    builder.accept(mock(SparkListenerApplicationStart.class), facets::put);
    assertThat(facets).isEmpty();
  }

  private static FabricSparkRunFacetBuilder builderFor(SparkConf conf) {
    SparkContext sparkContext = mock(SparkContext.class);
    when(sparkContext.getConf()).thenReturn(conf);
    return new FabricSparkRunFacetBuilder(context(sparkContext));
  }

  private static OpenLineageContext context(SparkContext sparkContext) {
    return OpenLineageContext.builder()
        .sparkContext(sparkContext)
        .openLineage(OL)
        .meterRegistry(new SimpleMeterRegistry())
        .openLineageConfig(new SparkOpenLineageConfig())
        .build();
  }
}
