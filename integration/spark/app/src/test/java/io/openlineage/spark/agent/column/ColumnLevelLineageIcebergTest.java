/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.column;

import static io.openlineage.spark.agent.column.ColumnLevelLineageTestUtils.assertColumnDependsOn;
import static io.openlineage.spark.agent.column.ColumnLevelLineageTestUtils.assertColumnDependsOnInputs;
import static org.apache.spark.sql.functions.col;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
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
import java.util.List;
import java.util.Optional;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.SparkSession$;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.QueryExecution;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd;
import org.apache.spark.sql.types.IntegerType$;
import org.apache.spark.sql.types.LongType$;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StringType$;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import scala.collection.immutable.HashMap;

@Slf4j
@Tag("iceberg")
class ColumnLevelLineageIcebergTest {

  @SuppressWarnings("PMD")
  private static final String LOCAL_IP = "127.0.0.1";

  private static final String INT_TYPE = "int";
  private static final String FILE = "file";
  private static final String T1_EXPECTED_NAME = "/tmp/column_level_lineage/db/t1";
  private static final String T2_EXPECTED_NAME = "/tmp/column_level_lineage/db/t2";
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
    DerbyUtils.loadSystemProperty(ColumnLevelLineageIcebergTest.class.getName());
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
            .config("spark.sql.catalog.local.warehouse", "/tmp/column_level_lineage/")
            .config("spark.sql.catalog.local.type", "hadoop")
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config("spark.driver.extraJavaOptions", "-Dderby.system.home=/tmp/col_v2/derby")
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
            .sparkExtensionVisitorWrapper(new SparkOpenLineageExtensionVisitorWrapper(config))
            .build();

    context
        .getColumnLevelLineageVisitors()
        .addAll(DatasetBuilderFactoryProvider.getInstance().getColumnLevelLineageVisitors(context));

    FileSystem.get(spark.sparkContext().hadoopConfiguration())
        .delete(new Path("/tmp/column_level_lineage/"), true);

    spark.sql("DROP TABLE IF EXISTS local.db.t1");
    spark.sql("DROP TABLE IF EXISTS local.db.t2");
    spark.sql("DROP TABLE IF EXISTS local.db.t");

    spark
        .createDataFrame(Arrays.asList(new GenericRow(new Object[] {1, 2})), structTypeSchema)
        .createOrReplaceTempView("temp");
  }

  @Test
  @SneakyThrows
  void testCreateTableAsSelectWithUnion() {
    spark.sql(CREATE_T1_FROM_TEMP);
    spark.sql("CREATE TABLE local.db.t2 USING iceberg AS SELECT * FROM temp");
    spark.sql(
        "CREATE TABLE local.db.t USING iceberg AS (SELECT * FROM local.db.t1 UNION SELECT * FROM local.db.t2)");

    LogicalPlan plan = LastQueryExecutionSparkEventListener.getLastExecutedLogicalPlan().get();
    when(queryExecution.optimizedPlan()).thenReturn(plan);
    OpenLineage.ColumnLineageDatasetFacet facet =
        ColumnLevelLineageUtils.buildColumnLineageDatasetFacet(event, context, schemaDatasetFacet)
            .get();

    assertColumnDependsOn(facet, "a", FILE, T1_EXPECTED_NAME, "a");
    assertColumnDependsOn(facet, "a", FILE, T2_EXPECTED_NAME, "a");
    assertColumnDependsOn(facet, "b", FILE, T2_EXPECTED_NAME, "b");
    assertColumnDependsOn(facet, "b", FILE, T1_EXPECTED_NAME, "b");

    // check that dataset name is a real path
    FileSystem local = FileSystem.get(spark.sparkContext().hadoopConfiguration());
    assertThat(local.exists(new Path(T1_EXPECTED_NAME))).isTrue();
    assertThat(local.exists(new Path(T1_EXPECTED_NAME))).isTrue();
    assertThat(local.listStatus(new Path(T1_EXPECTED_NAME)).length).isGreaterThan(0);
    assertThat(local.listStatus(new Path(T2_EXPECTED_NAME)).length).isGreaterThan(0);
  }

  @Test
  @SneakyThrows
  void testInsertIntoTableWithAlias() {
    spark.sql(CREATE_T1_FROM_TEMP);
    spark.sql("CREATE TABLE local.db.t2 USING iceberg AS SELECT * FROM temp");
    spark.sql("INSERT INTO local.db.t2 SELECT * FROM local.db.t1");

    LogicalPlan plan = LastQueryExecutionSparkEventListener.getLastExecutedLogicalPlan().get();
    when(queryExecution.optimizedPlan()).thenReturn(plan);
    OpenLineage.ColumnLineageDatasetFacet facet =
        ColumnLevelLineageUtils.buildColumnLineageDatasetFacet(event, context, schemaDatasetFacet)
            .get();

    assertColumnDependsOn(facet, "a", FILE, T1_EXPECTED_NAME, "a");
    assertColumnDependsOn(facet, "b", FILE, T1_EXPECTED_NAME, "b");
  }

  @Test
  void testUnaryExpression() {
    spark.sql(CREATE_T1_FROM_TEMP);
    spark.sql("INSERT INTO local.db.t1 VALUES (1,2),(3,4),(5,6)");
    spark.sql(
        "CREATE TABLE local.db.t2 USING iceberg AS (SELECT a, b, ceil(a) as `c`, abs(b) as `d` FROM local.db.t1)");

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
  }

  @Test
  void testEmptyColumnLineageFacet() {
    spark.sql(CREATE_T1_FROM_TEMP);
    spark.sql("INSERT INTO local.db.t1 VALUES (1,2),(3,4),(5,6)");
    spark.sql("CREATE TABLE local.db.t2 USING iceberg AS (SELECT * FROM local.db.t1)");

    OpenLineage.SchemaDatasetFacet wrongSchema =
        openLineage.newSchemaDatasetFacet(
            Arrays.asList(
                openLineage.newSchemaDatasetFacetFieldsBuilder().name("x").type(INT_TYPE).build(),
                openLineage.newSchemaDatasetFacetFieldsBuilder().name("y").type(INT_TYPE).build()));

    LogicalPlan plan = LastQueryExecutionSparkEventListener.getLastExecutedLogicalPlan().get();
    when(queryExecution.optimizedPlan()).thenReturn(plan);
    Optional<OpenLineage.ColumnLineageDatasetFacet> facet =
        ColumnLevelLineageUtils.buildColumnLineageDatasetFacet(event, context, wrongSchema);

    assertFalse(facet.isPresent());
  }

  @Test
  void testLogicalPlanUnavailable() {
    OpenLineageContext context = mock(OpenLineageContext.class);
    when(context.getQueryExecution()).thenReturn(Optional.empty());
    assertEquals(
        Optional.empty(),
        ColumnLevelLineageUtils.buildColumnLineageDatasetFacet(event, context, schemaDatasetFacet));
  }

  @Test
  void testLogicalPlanWhenOptimizedPlanIsNull() {
    OpenLineageContext context = mock(OpenLineageContext.class);
    QueryExecution queryExecution = mock(QueryExecution.class);
    when(context.getQueryExecution()).thenReturn(Optional.of(queryExecution));
    when(queryExecution.optimizedPlan()).thenReturn(null);
    assertEquals(
        Optional.empty(),
        ColumnLevelLineageUtils.buildColumnLineageDatasetFacet(event, context, schemaDatasetFacet));
  }

  @Test
  void testReadWriteParquetDataset() {
    Dataset<Long> df = spark.range(10);
    String inputDfPath = "/tmp/insertTestColumnLineage";
    df.write().mode(SaveMode.Overwrite).parquet(inputDfPath);

    Dataset<Row> df2 = spark.read().parquet(inputDfPath);

    df2.withColumn("col2", df2.col("id").multiply(2))
        .write()
        .mode(SaveMode.Overwrite)
        .parquet("/tmp/insertTestColumnLineage2");

    LogicalPlan plan = LastQueryExecutionSparkEventListener.getLastExecutedLogicalPlan().get();
    when(queryExecution.optimizedPlan()).thenReturn(plan);

    OpenLineage.SchemaDatasetFacet outputSchema =
        openLineage.newSchemaDatasetFacet(
            Arrays.asList(
                openLineage.newSchemaDatasetFacetFieldsBuilder().name("id").type("long").build(),
                openLineage
                    .newSchemaDatasetFacetFieldsBuilder()
                    .name("col2")
                    .type("long")
                    .build()));

    OpenLineage.ColumnLineageDatasetFacet facet =
        ColumnLevelLineageUtils.buildColumnLineageDatasetFacet(event, context, outputSchema).get();

    assertColumnDependsOn(facet, "id", FILE, inputDfPath, "id");
    assertColumnDependsOn(facet, "col2", FILE, inputDfPath, "id");
  }

  @Test
  void testBinaryAndComplexExpression() {
    spark.sql(CREATE_T1_FROM_TEMP);
    spark.sql("INSERT INTO local.db.t1 VALUES (1,2),(3,4),(5,6)");
    spark.sql(
        "CREATE TABLE local.db.t2 AS SELECT CONCAT(CAST(a AS STRING), CAST(b AS STRING)) as `c`, a+b as `d` FROM local.db.t1");

    OpenLineage.SchemaDatasetFacet outputSchema =
        openLineage.newSchemaDatasetFacet(
            Arrays.asList(
                openLineage.newSchemaDatasetFacetFieldsBuilder().name("c").type("string").build(),
                openLineage.newSchemaDatasetFacetFieldsBuilder().name("d").type("string").build()));

    LogicalPlan plan = LastQueryExecutionSparkEventListener.getLastExecutedLogicalPlan().get();
    when(queryExecution.optimizedPlan()).thenReturn(plan);
    OpenLineage.ColumnLineageDatasetFacet facet =
        ColumnLevelLineageUtils.buildColumnLineageDatasetFacet(event, context, outputSchema).get();

    assertColumnDependsOn(facet, "c", FILE, T1_EXPECTED_NAME, "a");
    assertColumnDependsOn(facet, "c", FILE, T1_EXPECTED_NAME, "b");
    assertColumnDependsOn(facet, "d", FILE, T1_EXPECTED_NAME, "a");
    assertColumnDependsOn(facet, "d", FILE, T1_EXPECTED_NAME, "b");
    assertColumnDependsOnInputs(facet, "c", 2);
    assertColumnDependsOnInputs(facet, "d", 2);
  }

  @Test
  void testJoinQuery() {
    spark.sql(CREATE_T1_FROM_TEMP);
    spark.sql("CREATE TABLE local.db.t2 USING iceberg AS SELECT * FROM temp");

    spark.sql(
        "CREATE TABLE local.db.t AS (SELECT (local.db.t1.a + local.db.t2.a) as c FROM local.db.t1 JOIN local.db.t2 ON local.db.t1.a = local.db.t2.a)");

    OpenLineage.SchemaDatasetFacet outputSchema =
        openLineage.newSchemaDatasetFacet(
            Arrays.asList(
                openLineage.newSchemaDatasetFacetFieldsBuilder().name("c").type(INT_TYPE).build()));

    LogicalPlan plan = LastQueryExecutionSparkEventListener.getLastExecutedLogicalPlan().get();
    when(queryExecution.optimizedPlan()).thenReturn(plan);
    OpenLineage.ColumnLineageDatasetFacet facet =
        ColumnLevelLineageUtils.buildColumnLineageDatasetFacet(event, context, outputSchema).get();

    assertColumnDependsOn(facet, "c", FILE, T1_EXPECTED_NAME, "a");
    assertColumnDependsOn(facet, "c", FILE, T2_EXPECTED_NAME, "a");
    assertColumnDependsOnInputs(facet, "c", 2);
  }

  @Test
  void testAggregateQuery() {
    spark.sql(CREATE_T1_FROM_TEMP);
    spark.sql("CREATE TABLE local.db.t2 AS (SELECT max(a) as a FROM local.db.t1 GROUP BY a)");

    OpenLineage.SchemaDatasetFacet outputSchema =
        openLineage.newSchemaDatasetFacet(
            Arrays.asList(
                openLineage.newSchemaDatasetFacetFieldsBuilder().name("a").type(INT_TYPE).build()));

    LogicalPlan plan = LastQueryExecutionSparkEventListener.getLastExecutedLogicalPlan().get();
    when(queryExecution.optimizedPlan()).thenReturn(plan);
    OpenLineage.ColumnLineageDatasetFacet facet =
        ColumnLevelLineageUtils.buildColumnLineageDatasetFacet(event, context, outputSchema).get();

    assertColumnDependsOn(facet, "a", FILE, T1_EXPECTED_NAME, "a");
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

  @Test
  void testJobWithCachedDataset() {
    spark.sql(CREATE_T1_FROM_TEMP);

    Dataset<Row> cachedDataset1 = spark.read().table("local.db.t1").cache();
    cachedDataset1.take(1);
    cachedDataset1.count(); // run some action to warm-up cache

    Dataset<Row> cachedDataset2 = cachedDataset1.select(col("a").as("c"), col("b").as("d")).cache();
    cachedDataset2.take(1);
    cachedDataset2.count();

    cachedDataset2.select(col("c").as("e"), col("d").as("f")).write().saveAsTable("local.db.t3");

    OpenLineage.SchemaDatasetFacet outputSchema =
        openLineage.newSchemaDatasetFacet(
            Arrays.asList(
                openLineage.newSchemaDatasetFacetFieldsBuilder().name("e").type(INT_TYPE).build(),
                openLineage.newSchemaDatasetFacetFieldsBuilder().name("f").type(INT_TYPE).build()));

    LogicalPlan plan = LastQueryExecutionSparkEventListener.getLastExecutedLogicalPlan().get();
    when(queryExecution.optimizedPlan()).thenReturn(plan);

    OpenLineage.ColumnLineageDatasetFacet facet =
        ColumnLevelLineageUtils.buildColumnLineageDatasetFacet(event, context, outputSchema).get();

    assertColumnDependsOn(facet, "e", FILE, T1_EXPECTED_NAME, "a");
    assertColumnDependsOn(facet, "f", FILE, T1_EXPECTED_NAME, "b");
    assertColumnDependsOnInputs(facet, "e", 1);
    assertColumnDependsOnInputs(facet, "f", 1);
  }

  @Test
  void testMergeInto() {
    Dataset<Row> dataset =
        spark
            .createDataFrame(
                ImmutableList.of(
                    RowFactory.create(1L, "bat"),
                    RowFactory.create(2L, "mouse"),
                    RowFactory.create(3L, "horse")),
                new StructType(
                    new StructField[] {
                      new StructField("a", LongType$.MODULE$, false, Metadata.empty()),
                      new StructField("b", StringType$.MODULE$, false, Metadata.empty())
                    }))
            .repartition(1);
    dataset.createOrReplaceTempView("temp");

    spark.sql("CREATE TABLE local.db.t1 USING iceberg AS SELECT * FROM temp");
    spark.sql("CREATE TABLE local.db.t2 USING iceberg AS SELECT * FROM temp");

    spark.sql(
        "MERGE INTO local.db.t1 USING local.db.t2 ON local.db.t1.a = local.db.t2.a"
            + " WHEN MATCHED THEN UPDATE SET *"
            + " WHEN NOT MATCHED THEN INSERT *");

    List<LogicalPlan> plans = LastQueryExecutionSparkEventListener.getExecutedLogicalPlans();

    LogicalPlan plan =
        plans.stream()
            .filter(
                p ->
                    p.getClass().getCanonicalName().endsWith("ReplaceIcebergData")
                        || p.getClass().getCanonicalName().endsWith("ReplaceData"))
            .findAny()
            .get();

    when(queryExecution.optimizedPlan()).thenReturn(plan);
    OpenLineage.ColumnLineageDatasetFacet facet =
        io.openlineage.spark3.agent.lifecycle.plan.column.ColumnLevelLineageUtils
            .buildColumnLineageDatasetFacet(event, context, schemaDatasetFacet)
            .get();

    assertColumnDependsOn(facet, "a", "file", T1_EXPECTED_NAME, "a");
    assertColumnDependsOn(facet, "b", "file", T1_EXPECTED_NAME, "b");

    assertColumnDependsOn(facet, "a", "file", T2_EXPECTED_NAME, "a");
    assertColumnDependsOn(facet, "b", "file", T2_EXPECTED_NAME, "b");
  }
}
