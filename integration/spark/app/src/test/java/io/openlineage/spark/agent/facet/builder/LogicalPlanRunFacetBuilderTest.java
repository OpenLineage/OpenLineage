/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.facet.builder;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import com.google.common.collect.ImmutableMap;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.RunFacet;
import io.openlineage.client.transports.FacetsConfig;
import io.openlineage.spark.agent.Versions;
import io.openlineage.spark.agent.facets.builder.LogicalPlanRunFacetBuilder;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.SparkOpenLineageConfig;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.scheduler.JobSucceeded$;
import org.apache.spark.scheduler.SparkListenerJobEnd;
import org.apache.spark.scheduler.SparkListenerJobStart;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.execution.QueryExecution;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart;
import org.apache.spark.sql.types.IntegerType$;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StringType$;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class LogicalPlanRunFacetBuilderTest {

  private static SparkContext sparkContext;
  private static SparkSession sparkSession;
  private static QueryExecution queryExecution;

  @BeforeAll
  public static void setup() {
    sparkContext =
        SparkContext.getOrCreate(
            new SparkConf().setAppName("LogicalPlanRunFacetBuilderTest").setMaster("local"));
    sparkSession = SparkSession.builder().sparkContext(sparkContext).getOrCreate();
    queryExecution =
        sparkSession
            .createDataFrame(
                Arrays.asList(new GenericRow(new Object[] {1, "hello"})),
                new StructType(
                    new StructField[] {
                      new StructField(
                          "count",
                          IntegerType$.MODULE$,
                          false,
                          new Metadata(new scala.collection.immutable.HashMap<>())),
                      new StructField(
                          "word",
                          StringType$.MODULE$,
                          false,
                          new Metadata(new scala.collection.immutable.HashMap<>()))
                    }))
            .queryExecution();
  }

  @AfterAll
  public static void tearDown() throws IOException {
    sparkSession.close();
    sparkContext.stop();
  }

  @Test
  void testIsDefined() {
    SparkOpenLineageConfig openLineageConfig = new SparkOpenLineageConfig();
    openLineageConfig.setFacetsConfig(new FacetsConfig());
    openLineageConfig
        .getFacetsConfig()
        .setDisabledFacets(ImmutableMap.of("spark.logicalPlan", false));
    LogicalPlanRunFacetBuilder builder =
        new LogicalPlanRunFacetBuilder(
            OpenLineageContext.builder()
                .sparkContext(sparkContext)
                .openLineage(new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI))
                .queryExecution(queryExecution)
                .openLineageConfig(openLineageConfig)
                .meterRegistry(new SimpleMeterRegistry())
                .build());
    assertThat(builder.isDefinedAt(mock(SparkListenerSQLExecutionStart.class))).isTrue();

    assertThat(builder.isDefinedAt(mock(SparkListenerSQLExecutionEnd.class))).isTrue();

    assertThat(builder.isDefinedAt(mock(SparkListenerJobEnd.class))).isTrue();

    assertThat(
            builder.isDefinedAt(
                new SparkListenerJobStart(
                    1, 1L, ScalaConversionUtils.asScalaSeqEmpty(), new Properties())))
        .isTrue();
  }

  @Test
  void testIsDefinedWhenFacetDisabled() {
    SparkOpenLineageConfig config = new SparkOpenLineageConfig();
    config.getFacetsConfig().setDisabledFacets(ImmutableMap.of("spark.logicalPlan", true));
    LogicalPlanRunFacetBuilder builder =
        new LogicalPlanRunFacetBuilder(
            OpenLineageContext.builder()
                .sparkContext(sparkContext)
                .openLineage(new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI))
                .queryExecution(queryExecution)
                .openLineageConfig(config)
                .meterRegistry(new SimpleMeterRegistry())
                .build());

    assertThat(
            builder.isDefinedAt(
                new SparkListenerJobStart(
                    1, 1L, ScalaConversionUtils.asScalaSeqEmpty(), new Properties())))
        .isFalse();
  }

  @Test
  void testIsNotDefinedWithoutQueryExecution() {
    LogicalPlanRunFacetBuilder builder =
        new LogicalPlanRunFacetBuilder(
            OpenLineageContext.builder()
                .sparkContext(sparkContext)
                .openLineage(new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI))
                .meterRegistry(new SimpleMeterRegistry())
                .openLineageConfig(new SparkOpenLineageConfig())
                .build());
    assertThat(builder.isDefinedAt(mock(SparkListenerSQLExecutionStart.class))).isFalse();

    assertThat(builder.isDefinedAt(mock(SparkListenerSQLExecutionEnd.class))).isFalse();

    assertThat(builder.isDefinedAt(new SparkListenerJobEnd(1, 1L, JobSucceeded$.MODULE$)))
        .isFalse();

    assertThat(
            builder.isDefinedAt(
                new SparkListenerJobStart(
                    1, 1L, ScalaConversionUtils.asScalaSeqEmpty(), new Properties())))
        .isFalse();
  }

  @Test
  void testBuild() {
    LogicalPlanRunFacetBuilder builder =
        new LogicalPlanRunFacetBuilder(
            OpenLineageContext.builder()
                .sparkContext(sparkContext)
                .openLineage(new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI))
                .queryExecution(queryExecution)
                .meterRegistry(new SimpleMeterRegistry())
                .openLineageConfig(new SparkOpenLineageConfig())
                .build());
    Map<String, RunFacet> facetMap = new HashMap<>();
    builder.build(mock(SparkListenerSQLExecutionEnd.class), facetMap::put);
    assertThat(facetMap)
        .hasEntrySatisfying(
            "spark.logicalPlan",
            facet ->
                assertThat(facet)
                    .hasFieldOrPropertyWithValue("plan", queryExecution.optimizedPlan().toJSON()));
  }
}
