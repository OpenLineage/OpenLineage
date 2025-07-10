/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.column;

import static io.openlineage.spark.agent.column.ColumnLevelLineageTestUtils.*;
import static io.openlineage.spark.agent.lifecycle.plan.column.TransformationInfo.Subtypes.FILTER;
import static io.openlineage.spark.agent.lifecycle.plan.column.TransformationInfo.Subtypes.GROUP_BY;
import static io.openlineage.spark.agent.lifecycle.plan.column.TransformationInfo.Subtypes.JOIN;
import static io.openlineage.spark.agent.lifecycle.plan.column.TransformationInfo.Subtypes.SORT;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.Versions;
import io.openlineage.spark.agent.lifecycle.SparkOpenLineageExtensionVisitorWrapper;
import io.openlineage.spark.agent.lifecycle.plan.column.TransformationInfo;
import io.openlineage.spark.agent.util.DerbyUtils;
import io.openlineage.spark.agent.util.LastQueryExecutionSparkEventListener;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.SparkOpenLineageConfig;
import io.openlineage.spark3.agent.lifecycle.plan.column.ColumnLevelLineageUtils;
import java.util.Arrays;
import java.util.UUID;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.SparkSession$;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.QueryExecution;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

// TODO #3084: Remove when the column lineage has dataset dependencies flag removed
@Slf4j
@EnabledIfSystemProperty(named = "spark.version", matches = "([34].*)")
@SuppressWarnings("PMD.JUnitTestContainsTooManyAsserts")
class ColumnLineageWithTransformationTypesOnlyFieldDependenciesTest {

  private static final String FILE = "file";

  @SuppressWarnings("PMD")
  private static final String LOCAL_IP = "127.0.0.1";

  private static final String T1_EXPECTED_NAME = "column_transformation_deprecated/t1";
  private static final String T2_EXPECTED_NAME = "column_transformation_deprecated/t2";
  private static final String T3_EXPECTED_NAME = "column_transformation_deprecated/t3";
  public static final String DATA_PATH = "/tmp/column_transformation_deprecated";
  SparkSession spark;
  OpenLineageContext context;
  SparkListenerEvent event = mock(SparkListenerSQLExecutionEnd.class);
  QueryExecution queryExecution = mock(QueryExecution.class);

  OpenLineage openLineage = new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI);

  @BeforeAll
  @SneakyThrows
  public static void beforeAll() {
    DerbyUtils.loadSystemProperty(
        ColumnLineageWithTransformationTypesOnlyFieldDependenciesTest.class.getName());
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
            .enableHiveSupport()
            .getOrCreate();

    SparkOpenLineageConfig config = new SparkOpenLineageConfig();
    config.getColumnLineageConfig().setDatasetLineageEnabled(false);
    context =
        OpenLineageContext.builder()
            .sparkSession(spark)
            .sparkContext(spark.sparkContext())
            .applicationName("ColumnLevelLineage")
            .applicationUuid(UUID.fromString("8d99e33e-bbbb-cccc-dddd-18f2343aaaaa"))
            .openLineage(new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI))
            .queryExecution(queryExecution)
            .meterRegistry(new SimpleMeterRegistry())
            .openLineageConfig(config)
            .sparkExtensionVisitorWrapper(mock(SparkOpenLineageExtensionVisitorWrapper.class))
            .build();

    FileSystem.get(spark.sparkContext().hadoopConfiguration()).delete(new Path(DATA_PATH), true);

    spark.sql("DROP TABLE IF EXISTS t1");
    spark.sql("DROP TABLE IF EXISTS t2");
    spark.sql("DROP TABLE IF EXISTS t3");
    spark.sql("DROP TABLE IF EXISTS t");
  }

  @Test
  void simpleQueryIndirect() {
    createTable("t1", "a;int", "b;int");
    OpenLineage.ColumnLineageDatasetFacet facet =
        getFacetForQuery(getSchemaFacet("a;int"), "SELECT a FROM t1 WHERE b > 1");
    assertCountColumnDependencies(facet, 2);
    assertColumnDependsOnType(
        facet, "a", FILE, T1_EXPECTED_NAME, "a", TransformationInfo.identity());
    assertColumnDependsOnType(
        facet, "a", FILE, T1_EXPECTED_NAME, "b", TransformationInfo.indirect(FILTER));
    assertCountDatasetDependencies(facet, 0);
  }

  @Test
  void simpleQueryMultipleIndirect() {
    createTable("t1", "a;int", "b;int", "c;int");
    OpenLineage.ColumnLineageDatasetFacet facet =
        getFacetForQuery(
            getSchemaFacet("a;int"), "SELECT a FROM t1 WHERE b > 1 GROUP BY a, c ORDER BY c");
    assertCountColumnDependencies(facet, 5);
    assertColumnDependsOnType(
        facet, "a", FILE, T1_EXPECTED_NAME, "a", TransformationInfo.identity());
    assertColumnDependsOnType(
        facet, "a", FILE, T1_EXPECTED_NAME, "a", TransformationInfo.indirect(GROUP_BY));
    assertColumnDependsOnType(
        facet, "a", FILE, T1_EXPECTED_NAME, "b", TransformationInfo.indirect(FILTER));
    assertColumnDependsOnType(
        facet, "a", FILE, T1_EXPECTED_NAME, "c", TransformationInfo.indirect(GROUP_BY));
    assertColumnDependsOnType(
        facet, "a", FILE, T1_EXPECTED_NAME, "c", TransformationInfo.indirect(SORT));
    assertCountDatasetDependencies(facet, 0);
  }

  @Test
  void simpleQueryMasking() {
    createTable("t1", "a;int", "b;int");
    OpenLineage.ColumnLineageDatasetFacet facet =
        getFacetForQuery(
            getSchemaFacet("i;int", "t;int", "mt;string", "a;int", "ma;string"),
            "SELECT "
                + "a as i, "
                + "a + 1 as t, "
                + "sha1(string(a + 1)) as mt, "
                + "sum(b) as a, "
                + "sha1(string(sum(b))) as ma "
                + "FROM t1 GROUP BY a");
    assertCountColumnDependencies(facet, 10);
    assertColumnDependsOnType(
        facet, "i", FILE, T1_EXPECTED_NAME, "a", TransformationInfo.identity());
    assertColumnDependsOnType(
        facet, "i", FILE, T1_EXPECTED_NAME, "a", TransformationInfo.indirect(GROUP_BY));
    assertColumnDependsOnType(
        facet, "t", FILE, T1_EXPECTED_NAME, "a", TransformationInfo.transformation());
    assertColumnDependsOnType(
        facet, "t", FILE, T1_EXPECTED_NAME, "a", TransformationInfo.indirect(GROUP_BY));
    assertColumnDependsOnType(
        facet, "mt", FILE, T1_EXPECTED_NAME, "a", TransformationInfo.transformation(true));
    assertColumnDependsOnType(
        facet, "mt", FILE, T1_EXPECTED_NAME, "a", TransformationInfo.indirect(GROUP_BY));
    assertColumnDependsOnType(
        facet, "a", FILE, T1_EXPECTED_NAME, "b", TransformationInfo.aggregation());
    assertColumnDependsOnType(
        facet, "a", FILE, T1_EXPECTED_NAME, "a", TransformationInfo.indirect(GROUP_BY));
    assertColumnDependsOnType(
        facet, "ma", FILE, T1_EXPECTED_NAME, "b", TransformationInfo.aggregation(true));
    assertColumnDependsOnType(
        facet, "ma", FILE, T1_EXPECTED_NAME, "a", TransformationInfo.indirect(GROUP_BY));
    assertCountDatasetDependencies(facet, 0);
  }

  @Test
  void complexQueryCTEJoinsFilter() {
    createTable("t1", "a;int", "b;string");
    createTable("t2", "a;int", "c;int");
    createTable("t3", "a;int", "d;int");
    OpenLineage.ColumnLineageDatasetFacet facet =
        getFacetForQuery(
            getSchemaFacet("a;int", "b;string", "c;int", "d;int"),
            "WITH tmp as (SELECT * FROM t1 where b = '1'),\n "
                + "tmp2 as (SELECT * FROM t2 where c = 1),\n "
                + "tmp3 as (SELECT tmp.a, b, c from tmp join tmp2 on tmp.a = tmp2.a)\n "
                + "SELECT tmp3.a as a, b, c, d FROM tmp3 join t3 on tmp3.a = t3.a order by d");
    // TODO: There are appears to be bug here.
    //  Normally this should work: assertCountColumnDependencies(facet, 28);
    //  However, there appears to be 44 dataset dependencies (i.e. 16 too many).
    assertColumnDependsOnType(
        facet, "a", FILE, T1_EXPECTED_NAME, "a", TransformationInfo.identity());
    assertColumnDependsOnType(
        facet, "b", FILE, T1_EXPECTED_NAME, "b", TransformationInfo.identity());
    assertColumnDependsOnType(
        facet, "c", FILE, T2_EXPECTED_NAME, "c", TransformationInfo.identity());
    assertColumnDependsOnType(
        facet, "d", FILE, T3_EXPECTED_NAME, "d", TransformationInfo.identity());
    assertAllColumnsDependsOnType(
        facet,
        Arrays.asList("a", "b", "c", "d"),
        FILE,
        T1_EXPECTED_NAME,
        "a",
        TransformationInfo.indirect(JOIN));
    assertAllColumnsDependsOnType(
        facet,
        Arrays.asList("a", "b", "c", "d"),
        FILE,
        T2_EXPECTED_NAME,
        "a",
        TransformationInfo.indirect(JOIN));
    assertAllColumnsDependsOnType(
        facet,
        Arrays.asList("a", "b", "c", "d"),
        FILE,
        T3_EXPECTED_NAME,
        "a",
        TransformationInfo.indirect(JOIN));
    assertAllColumnsDependsOnType(
        facet,
        Arrays.asList("a", "b", "c", "d"),
        FILE,
        T1_EXPECTED_NAME,
        "b",
        TransformationInfo.indirect(FILTER));
    assertAllColumnsDependsOnType(
        facet,
        Arrays.asList("a", "b", "c", "d"),
        FILE,
        T2_EXPECTED_NAME,
        "c",
        TransformationInfo.indirect(FILTER));
    assertAllColumnsDependsOnType(
        facet,
        Arrays.asList("a", "b", "c", "d"),
        FILE,
        T3_EXPECTED_NAME,
        "d",
        TransformationInfo.indirect(SORT));
    assertCountDatasetDependencies(facet, 0);
  }

  @NotNull
  private OpenLineage.ColumnLineageDatasetFacet getFacetForQuery(
      OpenLineage.SchemaDatasetFacet schemaFacet, String query) {
    Dataset<Row> sql = spark.sql(query);

    OpenLineage.ColumnLineageDatasetFacet facet = getColumnLineageDatasetFacet(schemaFacet, sql);
    return facet;
  }

  private void createTable(String table, String... fields) {
    spark.sql(
        String.format(
            "CREATE TABLE %s (%s) LOCATION '%s/%s'",
            table,
            Arrays.stream(fields).map(e -> e.replace(";", " ")).collect(Collectors.joining(", ")),
            DATA_PATH,
            table));
    spark.sql(
        String.format(
            "INSERT INTO %s VALUES (%s)",
            table,
            Arrays.stream(fields)
                .map(e -> "string".equals(e.split(";")[1]) ? "\"1\"" : "1")
                .collect(Collectors.joining(","))));
  }

  @NotNull
  private OpenLineage.ColumnLineageDatasetFacet getColumnLineageDatasetFacet(
      OpenLineage.SchemaDatasetFacet schemaFacet, Dataset<Row> sql) {
    sql.collect(); // trigger execution

    LogicalPlan plan = LastQueryExecutionSparkEventListener.getLastExecutedLogicalPlan().get();
    when(queryExecution.optimizedPlan()).thenReturn(plan);
    OpenLineage.ColumnLineageDatasetFacet facet =
        ColumnLevelLineageUtils.buildColumnLineageDatasetFacet(event, context, schemaFacet).get();
    return facet;
  }

  private OpenLineage.SchemaDatasetFacet getSchemaFacet(String... fields) {
    return openLineage.newSchemaDatasetFacet(
        Arrays.stream(fields)
            .map(
                f ->
                    openLineage
                        .newSchemaDatasetFacetFieldsBuilder()
                        .name(f.split(";")[0])
                        .type(f.split(";")[1])
                        .build())
            .collect(Collectors.toList()));
  }
}
