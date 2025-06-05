/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.column;

import static io.openlineage.spark.agent.column.ColumnLevelLineageTestUtils.assertColumnDependsOn;
import static io.openlineage.spark.agent.column.ColumnLevelLineageTestUtils.assertColumnDependsOnInputs;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.Versions;
import io.openlineage.spark.agent.lifecycle.DatasetBuilderFactoryProvider;
import io.openlineage.spark.agent.lifecycle.SparkOpenLineageExtensionVisitorWrapper;
import io.openlineage.spark.agent.lifecycle.plan.column.ColumnLevelLineageUtils;
import io.openlineage.spark.agent.util.DerbyUtils;
import io.openlineage.spark.agent.util.LastQueryExecutionSparkEventListener;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.SparkOpenLineageConfig;
import java.util.Arrays;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.SparkSession$;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.QueryExecution;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd;
import org.apache.spark.sql.types.IntegerType$;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import scala.collection.immutable.HashMap;

// TODO #3084: Remove when the column lineage has dataset dependencies flag removed
@Slf4j
@Tag("iceberg")
class ColumnLevelLineageIcebergOnlyFieldDependenciesTest {

  @SuppressWarnings("PMD")
  private static final String LOCAL_IP = "127.0.0.1";

  private static final String INT_TYPE = "int";
  private static final String FILE = "file";
  private static final String T1_EXPECTED_NAME = "/tmp/column_level_lineage_deprecated/db/t1";
  private static final String CREATE_T1_FROM_TEMP =
      "CREATE TABLE local.db.t1 USING iceberg AS SELECT * FROM temp";
  SparkSession spark;
  QueryExecution queryExecution = mock(QueryExecution.class);

  OpenLineageContext context;
  SparkListenerEvent event = mock(SparkListenerSQLExecutionEnd.class);
  OpenLineage openLineage = new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI);
  OpenLineage.SchemaDatasetFacet schemaDatasetFacet =
      openLineage.newSchemaDatasetFacet(
          Arrays.asList(
              openLineage.newSchemaDatasetFacetFieldsBuilder().name("a").type(INT_TYPE).build(),
              openLineage.newSchemaDatasetFacetFieldsBuilder().name("b").type(INT_TYPE).build()));
  StructType structTypeSchema =
      new StructType(
          new StructField[] {
            new StructField("a", IntegerType$.MODULE$, false, new Metadata(new HashMap<>())),
            new StructField("b", IntegerType$.MODULE$, false, new Metadata(new HashMap<>()))
          });

  @BeforeAll
  @SneakyThrows
  public static void beforeAll() {
    DerbyUtils.loadSystemProperty(
        ColumnLevelLineageIcebergOnlyFieldDependenciesTest.class.getName());
    SparkSession$.MODULE$.cleanupAnyExistingSession();
  }

  @AfterAll
  @SneakyThrows
  public static void afterAll() {
    DerbyUtils.clearDerbyProperty();
    SparkSession$.MODULE$.cleanupAnyExistingSession();
  }

  @BeforeEach
  @SneakyThrows
  public void beforeEach() {
    spark =
        SparkSession.builder()
            .master("local[*]")
            .appName("ColumnLevelLineage")
            .config("spark.extraListeners", LastQueryExecutionSparkEventListener.class.getName())
            .config("spark.driver.host", LOCAL_IP)
            .config("spark.driver.bindAddress", LOCAL_IP)
            .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.spark_catalog.type", "hive")
            .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.local.warehouse", "/tmp/column_level_lineage_deprecated/")
            .config("spark.sql.catalog.local.type", "hadoop")
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config("spark.driver.extraJavaOptions", "-Dderby.system.home=/tmp/col_v2/derby")
            .getOrCreate();

    SparkOpenLineageConfig config = new SparkOpenLineageConfig();
    config.getColumnLineageConfig().setDatasetLineageEnabled(false);
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

    context
        .getColumnLevelLineageVisitors()
        .addAll(DatasetBuilderFactoryProvider.getInstance().getColumnLevelLineageVisitors(context));

    FileSystem.get(spark.sparkContext().hadoopConfiguration())
        .delete(new Path("/tmp/column_level_lineage_deprecated/"), true);

    spark.sql("DROP TABLE IF EXISTS local.db.t1");

    spark
        .createDataFrame(Arrays.asList(new GenericRow(new Object[] {1, 2})), structTypeSchema)
        .createOrReplaceTempView("temp");
  }

  @Test
  void testCTEQuery() {
    spark.sql(CREATE_T1_FROM_TEMP);
    spark
        .sql(
            "WITH t2(a,b) AS (SELECT * FROM local.db.t1) SELECT a AS c, b AS d FROM t2 WHERE t2.a = 1")
        .collect();

    OpenLineage.SchemaDatasetFacet outputSchema =
        openLineage.newSchemaDatasetFacet(
            Arrays.asList(
                openLineage.newSchemaDatasetFacetFieldsBuilder().name("c").type(INT_TYPE).build(),
                openLineage.newSchemaDatasetFacetFieldsBuilder().name("d").type(INT_TYPE).build()));

    LogicalPlan plan = LastQueryExecutionSparkEventListener.getLastExecutedLogicalPlan().get();
    when(queryExecution.optimizedPlan()).thenReturn(plan);
    OpenLineage.ColumnLineageDatasetFacet facet =
        ColumnLevelLineageUtils.buildColumnLineageDatasetFacet(event, context, outputSchema).get();

    assertColumnDependsOn(facet, "c", FILE, T1_EXPECTED_NAME, "a");
    assertColumnDependsOn(facet, "d", FILE, T1_EXPECTED_NAME, "b");
    assertColumnDependsOnInputs(facet, "c", 1);
    assertColumnDependsOnInputs(facet, "d", 2);
  }
}
