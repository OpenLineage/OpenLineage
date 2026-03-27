/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.column;

import static org.apache.spark.sql.functions.*;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.Spark4CompatUtils;
import io.openlineage.spark.agent.Versions;
import io.openlineage.spark.agent.lifecycle.SparkOpenLineageExtensionVisitorWrapper;
import io.openlineage.spark.agent.util.DerbyUtils;
import io.openlineage.spark.agent.util.LastQueryExecutionSparkEventListener;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.SparkOpenLineageConfig;
import io.openlineage.spark3.agent.lifecycle.plan.column.ColumnLevelLineageUtils;
import java.util.Arrays;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.QueryExecution;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

@Slf4j
@EnabledIfSystemProperty(named = "spark.version", matches = "([34].*)")
class ColumnLevelLineageInMemoryTest {

  @SuppressWarnings("PMD")
  private static final String LOCAL_IP = "127.0.0.1";

  SparkSession spark;
  OpenLineageContext context;
  SparkListenerEvent event = mock(SparkListenerSQLExecutionEnd.class);
  QueryExecution queryExecution = mock(QueryExecution.class);

  OpenLineage openLineage = new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI);
  OpenLineage.SchemaDatasetFacet schemaDatasetFacet =
      openLineage.newSchemaDatasetFacet(
          Arrays.asList(
              openLineage.newSchemaDatasetFacetFieldsBuilder().name("a").type("int").build(),
              openLineage.newSchemaDatasetFacetFieldsBuilder().name("b").type("int").build()));

  @BeforeAll
  @SneakyThrows
  public static void beforeAll() {
    DerbyUtils.loadSystemProperty(ColumnLevelLineageInMemoryTest.class.getName());
    Spark4CompatUtils.cleanupAnyExistingSession();
  }

  @AfterAll
  @SneakyThrows
  public static void afterAll() {
    DerbyUtils.clearDerbyProperty();
    Spark4CompatUtils.cleanupAnyExistingSession();
  }

  @BeforeEach
  @SneakyThrows
  public void beforeEach() {
    spark =
        Spark4CompatUtils.builderWithHiveSupport()
            .master("local[*]")
            .appName("ColumnLevelLineage")
            .config("spark.extraListeners", LastQueryExecutionSparkEventListener.class.getName())
            .config("spark.driver.host", LOCAL_IP)
            .config("spark.driver.bindAddress", LOCAL_IP)
            .getOrCreate();

    SparkOpenLineageConfig config = new SparkOpenLineageConfig();
    context =
        OpenLineageContext.builder()
            .sparkSession(spark)
            .sparkContext(spark.sparkContext())
            .openLineage(new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI))
            .queryExecution(queryExecution)
            .meterRegistry(new SimpleMeterRegistry())
            .openLineageConfig(config)
            .sparkExtensionVisitorWrapper(mock(SparkOpenLineageExtensionVisitorWrapper.class))
            .build();
  }

  @Test
  void testCachedDataframe() {
    Dataset<Long> left = spark.range(1, 11);
    Dataset<Long> right = spark.range(5, 16);

    // Explicit join condition keeps both sides' columns → duplicate names in schema
    Dataset<Row> joined =
        left.alias("l").join(right.alias("r"), col("l.id").equalTo(col("r.id")), "full_outer");

    // Cache creates InMemoryRelation with duplicate attribute names in output()
    Dataset<Row> cached = joined.cache();
    cached.select(col("l.id").alias("left"), col("r.id").alias("right")).collectAsList();

    LogicalPlan plan = LastQueryExecutionSparkEventListener.getLastExecutedLogicalPlan().get();
    when(queryExecution.optimizedPlan()).thenReturn(plan);
    assertDoesNotThrow(
        () ->
            ColumnLevelLineageUtils.buildColumnLineageDatasetFacet(
                event, context, schemaDatasetFacet));
  }
}
