/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.column;

import static io.openlineage.client.utils.TransformationInfo.Subtypes.CONDITIONAL;
import static io.openlineage.client.utils.TransformationInfo.Subtypes.FILTER;
import static io.openlineage.client.utils.TransformationInfo.Subtypes.GROUP_BY;
import static io.openlineage.client.utils.TransformationInfo.Subtypes.SORT;
import static io.openlineage.client.utils.TransformationInfo.Subtypes.WINDOW;
import static io.openlineage.spark.agent.column.ColumnLevelLineageTestUtils.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.openlineage.client.OpenLineage;
import io.openlineage.client.utils.TransformationInfo;
import io.openlineage.spark.agent.Spark4CompatUtils;
import io.openlineage.spark.agent.Versions;
import io.openlineage.spark.agent.lifecycle.SparkOpenLineageExtensionVisitorWrapper;
import io.openlineage.spark.agent.util.DerbyUtils;
import io.openlineage.spark.agent.util.LastQueryExecutionSparkEventListener;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.SparkOpenLineageConfig;
import io.openlineage.spark3.agent.lifecycle.plan.column.ColumnLevelLineageUtils;
import java.util.Arrays;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.QueryExecution;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

@Slf4j
@EnabledIfSystemProperty(named = "spark.version", matches = "([34].*)")
@SuppressWarnings("PMD.JUnitTestContainsTooManyAsserts")
class ColumnLineageWithTransformationTypesTest {

  private static final String FILE = "file";

  @SuppressWarnings("PMD")
  private static final String LOCAL_IP = "127.0.0.1";

  private static final String T1_EXPECTED_NAME = "column_transformation/t1";
  private static final String T2_EXPECTED_NAME = "column_transformation/t2";
  public static final String DATA_PATH = "/tmp/column_transformation";
  SparkSession spark;
  OpenLineageContext context;
  SparkListenerEvent event = mock(SparkListenerSQLExecutionEnd.class);
  QueryExecution queryExecution = mock(QueryExecution.class);

  OpenLineage openLineage = new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI);

  @BeforeAll
  @SneakyThrows
  public static void beforeAll() {
    DerbyUtils.loadSystemProperty(ColumnLineageWithTransformationTypesTest.class.getName());
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
    config.getColumnLineageConfig().setDatasetLineageEnabled(true);
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

    FileSystem.get(spark.sparkContext().hadoopConfiguration()).delete(new Path(DATA_PATH), true);

    spark.sql("DROP TABLE IF EXISTS t1");
    spark.sql("DROP TABLE IF EXISTS t2");
    spark.sql("DROP TABLE IF EXISTS t3");
    spark.sql("DROP TABLE IF EXISTS t4");
    spark.sql("DROP TABLE IF EXISTS t");
  }

  @Test
  void simpleQueryOnlyIdentity() {
    createTable("t1", "a;int");
    OpenLineage.ColumnLineageDatasetFacet facet =
        getFacetForQuery(getSchemaFacet("a;int"), "SELECT a FROM t1");
    assertCountColumnDependencies(facet, 1);
    assertColumnDependsOnType(
        facet, "a", FILE, T1_EXPECTED_NAME, "a", TransformationInfo.identity("a"));
    assertCountDatasetDependencies(facet, 0);
  }

  @Test
  void simpleQueryOnlyTransform() {
    createTable("t1", "a;int", "b;int");
    OpenLineage.ColumnLineageDatasetFacet facet =
        getFacetForQuery(
            getSchemaFacet("a;int", "b;int"), "SELECT concat(a, 'test') AS a, a+b as b FROM t1");
    assertCountColumnDependencies(facet, 3);
    assertColumnDependsOnType(
        facet,
        "a",
        FILE,
        T1_EXPECTED_NAME,
        "a",
        TransformationInfo.transformation(
            "concat(CAST(spark_catalog.default.t1.a AS STRING), 'test') AS a"));
    assertColumnDependsOnType(
        facet,
        "b",
        FILE,
        T1_EXPECTED_NAME,
        "a",
        TransformationInfo.transformation(
            "(spark_catalog.default.t1.a + spark_catalog.default.t1.b) AS b"));
    assertColumnDependsOnType(
        facet,
        "b",
        FILE,
        T1_EXPECTED_NAME,
        "b",
        TransformationInfo.transformation(
            "(spark_catalog.default.t1.a + spark_catalog.default.t1.b) AS b"));
    assertCountDatasetDependencies(facet, 0);
  }

  @Test
  void simpleQueryOnlyAggregation() {
    createTable("t1", "a;int");
    OpenLineage.ColumnLineageDatasetFacet facet =
        getFacetForQuery(getSchemaFacet("a;int"), "SELECT count(a) AS a FROM t1");
    assertCountColumnDependencies(facet, 1);
    assertColumnDependsOnType(
        facet,
        "a",
        FILE,
        T1_EXPECTED_NAME,
        "a",
        TransformationInfo.aggregation("count(spark_catalog.default.t1.a) AS a", true));
    assertCountDatasetDependencies(facet, 0);
  }

  @Test
  void simpleQueryIndirect() {
    createTable("t1", "a;int", "b;int");
    OpenLineage.ColumnLineageDatasetFacet facet =
        getFacetForQuery(getSchemaFacet("a;int"), "SELECT a FROM t1 WHERE b > 1");
    assertCountColumnDependencies(facet, 1);
    assertColumnDependsOnType(
        facet, "a", FILE, T1_EXPECTED_NAME, "a", TransformationInfo.identity("a"));
    assertCountDatasetDependencies(facet, 1);
    assertDatasetDependsOnType(
        facet,
        FILE,
        T1_EXPECTED_NAME,
        "b",
        TransformationInfo.indirect(
            FILTER,
            "WHERE ((spark_catalog.default.t1.b IS NOT NULL) AND (spark_catalog.default.t1.b > 1))"));
  }

  @Test
  void simpleQueryMultipleIndirect() {
    createTable("t1", "a;int", "b;int", "c;int");
    OpenLineage.ColumnLineageDatasetFacet facet =
        getFacetForQuery(
            getSchemaFacet("a;int", "c;int"),
            "SELECT a, c FROM t1 WHERE b > 1 GROUP BY a, c ORDER BY c");
    assertCountColumnDependencies(facet, 2);
    assertColumnDependsOnType(
        facet, "a", FILE, T1_EXPECTED_NAME, "a", TransformationInfo.identity("a"));
    assertColumnDependsOnType(
        facet, "c", FILE, T1_EXPECTED_NAME, "c", TransformationInfo.identity("c"));
    assertCountDatasetDependencies(facet, 4);
    assertDatasetDependsOnType(
        facet,
        FILE,
        T1_EXPECTED_NAME,
        "a",
        TransformationInfo.indirect(
            GROUP_BY, "GROUP BY spark_catalog.default.t1.a, spark_catalog.default.t1.c"));
    assertDatasetDependsOnType(
        facet,
        FILE,
        T1_EXPECTED_NAME,
        "b",
        TransformationInfo.indirect(
            FILTER,
            "WHERE ((spark_catalog.default.t1.b IS NOT NULL) AND (spark_catalog.default.t1.b > 1))"));
    assertDatasetDependsOnType(
        facet,
        FILE,
        T1_EXPECTED_NAME,
        "c",
        TransformationInfo.indirect(
            GROUP_BY, "GROUP BY spark_catalog.default.t1.a, spark_catalog.default.t1.c"));
    assertDatasetDependsOnType(
        facet,
        FILE,
        T1_EXPECTED_NAME,
        "c",
        TransformationInfo.indirect(SORT, "SORT BY spark_catalog.default.t1.c ASC NULLS FIRST"));
  }

  @Test
  void simpleQueryPriorityDirect() {
    createTable("t1", "a;int", "b;int");
    OpenLineage.ColumnLineageDatasetFacet facet =
        getFacetForQuery(
            getSchemaFacet("i;int", "t;int", "a;int", "ta;int", "tat;int"),
            "SELECT a as i, a + 1 as t, sum(b) as a, 2 * sum(b) as ta, 2 * sum(b + 3) as tat FROM t1 GROUP BY a");
    assertCountColumnDependencies(facet, 5);
    assertColumnDependsOnType(
        facet,
        "i",
        FILE,
        T1_EXPECTED_NAME,
        "a",
        TransformationInfo.identity("spark_catalog.default.t1.a AS i"));
    assertColumnDependsOnType(
        facet,
        "t",
        FILE,
        T1_EXPECTED_NAME,
        "a",
        TransformationInfo.transformation("(spark_catalog.default.t1.a + 1) AS t"));
    assertColumnDependsOnType(
        facet,
        "a",
        FILE,
        T1_EXPECTED_NAME,
        "b",
        TransformationInfo.aggregation("sum(spark_catalog.default.t1.b) AS a"));
    // Different order of nodes in plans between Spark 3 and 4 generate different descriptions, need
    // to check both
    assertColumnDependsOnType(
        facet,
        "ta",
        FILE,
        T1_EXPECTED_NAME,
        "b",
        TransformationInfo.aggregation(),
        Arrays.asList(
            "(sum(spark_catalog.default.t1.b) * 2L) AS ta",
            "(2L * sum(spark_catalog.default.t1.b)) AS ta"));
    assertColumnDependsOnType(
        facet,
        "tat",
        FILE,
        T1_EXPECTED_NAME,
        "b",
        TransformationInfo.aggregation(),
        Arrays.asList(
            "(sum((spark_catalog.default.t1.b + 3)) * 2L) AS tat",
            "(2L * sum((spark_catalog.default.t1.b + 3))) AS tat"));
    assertCountDatasetDependencies(facet, 1);
    assertDatasetDependsOnType(
        facet,
        FILE,
        T1_EXPECTED_NAME,
        "a",
        TransformationInfo.indirect(GROUP_BY, "GROUP BY spark_catalog.default.t1.a"));
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
    assertCountColumnDependencies(facet, 5);
    assertColumnDependsOnType(
        facet,
        "i",
        FILE,
        T1_EXPECTED_NAME,
        "a",
        TransformationInfo.identity("spark_catalog.default.t1.a AS i"));
    assertColumnDependsOnType(
        facet,
        "t",
        FILE,
        T1_EXPECTED_NAME,
        "a",
        TransformationInfo.transformation("(spark_catalog.default.t1.a + 1) AS t"));
    assertColumnDependsOnType(
        facet,
        "mt",
        FILE,
        T1_EXPECTED_NAME,
        "a",
        TransformationInfo.transformation(
            "sha1(CAST(CAST((spark_catalog.default.t1.a + 1) AS STRING) AS BINARY)) AS mt", true));
    assertColumnDependsOnType(
        facet,
        "a",
        FILE,
        T1_EXPECTED_NAME,
        "b",
        TransformationInfo.aggregation("sum(spark_catalog.default.t1.b) AS a"));
    assertColumnDependsOnType(
        facet,
        "ma",
        FILE,
        T1_EXPECTED_NAME,
        "b",
        TransformationInfo.aggregation(
            "sha1(CAST(CAST(sum(spark_catalog.default.t1.b) AS STRING) AS BINARY)) AS ma", true));
    assertCountDatasetDependencies(facet, 1);
    assertDatasetDependsOnType(
        facet,
        FILE,
        T1_EXPECTED_NAME,
        "a",
        TransformationInfo.indirect(GROUP_BY, "GROUP BY spark_catalog.default.t1.a"));
  }

  @Test
  void simpleQueryWithCaseWhenConditional() {
    createTable("t1", "a;int", "b;int");
    OpenLineage.ColumnLineageDatasetFacet facet =
        getFacetForQuery(
            getSchemaFacet("cond;int"),
            "SELECT CASE WHEN b > 1 THEN a ELSE a + b END AS cond FROM t1");
    assertCountColumnDependencies(facet, 3);
    assertColumnDependsOnType(
        facet,
        "cond",
        FILE,
        T1_EXPECTED_NAME,
        "a",
        TransformationInfo.transformation(
            "CASE WHEN (spark_catalog.default.t1.b > 1) THEN spark_catalog.default.t1.a ELSE (spark_catalog.default.t1.a + spark_catalog.default.t1.b) END AS cond"));
    assertColumnDependsOnType(
        facet,
        "cond",
        FILE,
        T1_EXPECTED_NAME,
        "b",
        TransformationInfo.transformation(
            "CASE WHEN (spark_catalog.default.t1.b > 1) THEN spark_catalog.default.t1.a ELSE (spark_catalog.default.t1.a + spark_catalog.default.t1.b) END AS cond"));
    assertColumnDependsOnType(
        facet,
        "cond",
        FILE,
        T1_EXPECTED_NAME,
        "b",
        TransformationInfo.indirect(
            CONDITIONAL,
            "CASE WHEN (spark_catalog.default.t1.b > 1) THEN spark_catalog.default.t1.a ELSE (spark_catalog.default.t1.a + spark_catalog.default.t1.b) END AS cond"));
    assertCountDatasetDependencies(facet, 0);
  }

  @Test
  void simpleQueryWithIfConditional() {
    createTable("t1", "a;int", "b;int");
    OpenLineage.ColumnLineageDatasetFacet facet =
        getFacetForQuery(getSchemaFacet("cond;int"), "SELECT IF(b > 1, a, a + b) AS cond FROM t1");
    assertCountColumnDependencies(facet, 3);
    assertColumnDependsOnType(
        facet,
        "cond",
        FILE,
        T1_EXPECTED_NAME,
        "a",
        TransformationInfo.transformation(
            "(IF((spark_catalog.default.t1.b > 1), spark_catalog.default.t1.a, (spark_catalog.default.t1.a + spark_catalog.default.t1.b))) AS cond"));
    assertColumnDependsOnType(
        facet,
        "cond",
        FILE,
        T1_EXPECTED_NAME,
        "b",
        TransformationInfo.transformation(
            "(IF((spark_catalog.default.t1.b > 1), spark_catalog.default.t1.a, (spark_catalog.default.t1.a + spark_catalog.default.t1.b))) AS cond"));
    assertColumnDependsOnType(
        facet,
        "cond",
        FILE,
        T1_EXPECTED_NAME,
        "b",
        TransformationInfo.indirect(
            CONDITIONAL,
            "(IF((spark_catalog.default.t1.b > 1), spark_catalog.default.t1.a, (spark_catalog.default.t1.a + spark_catalog.default.t1.b))) AS cond"));
    assertCountDatasetDependencies(facet, 0);
  }

  @Test
  void simpleQueryWithNullIfConditional() {
    createTable("t1", "a;int", "b;int");
    OpenLineage.ColumnLineageDatasetFacet facet =
        getFacetForQuery(getSchemaFacet("cond;int"), "SELECT NULLIF(a, b) AS cond FROM t1");
    assertCountColumnDependencies(facet, 3);
    assertColumnDependsOnType(
        facet,
        "cond",
        FILE,
        T1_EXPECTED_NAME,
        "a",
        TransformationInfo.transformation(
            "(IF((spark_catalog.default.t1.a = spark_catalog.default.t1.b), CAST(NULL AS INT), spark_catalog.default.t1.a)) AS cond"));
    assertColumnDependsOnType(
        facet,
        "cond",
        FILE,
        T1_EXPECTED_NAME,
        "a",
        TransformationInfo.indirect(
            CONDITIONAL,
            "(IF((spark_catalog.default.t1.a = spark_catalog.default.t1.b), CAST(NULL AS INT), spark_catalog.default.t1.a)) AS cond"));
    assertColumnDependsOnType(
        facet,
        "cond",
        FILE,
        T1_EXPECTED_NAME,
        "b",
        TransformationInfo.indirect(
            CONDITIONAL,
            "(IF((spark_catalog.default.t1.a = spark_catalog.default.t1.b), CAST(NULL AS INT), spark_catalog.default.t1.a)) AS cond"));
    assertCountDatasetDependencies(facet, 0);
  }

  @Test
  void simpleQueryWithNvlConditional() {
    createTable("t1", "a;int", "b;int");
    OpenLineage.ColumnLineageDatasetFacet facet =
        getFacetForQuery(getSchemaFacet("cond;int"), "SELECT NVL(a, b) AS cond FROM t1");
    assertCountColumnDependencies(facet, 4);
    assertColumnDependsOnType(
        facet,
        "cond",
        FILE,
        T1_EXPECTED_NAME,
        "a",
        TransformationInfo.transformation(
            "coalesce(spark_catalog.default.t1.a, spark_catalog.default.t1.b) AS cond"));
    assertColumnDependsOnType(
        facet,
        "cond",
        FILE,
        T1_EXPECTED_NAME,
        "a",
        TransformationInfo.indirect(
            CONDITIONAL,
            "coalesce(spark_catalog.default.t1.a, spark_catalog.default.t1.b) AS cond"));
    assertColumnDependsOnType(
        facet,
        "cond",
        FILE,
        T1_EXPECTED_NAME,
        "b",
        TransformationInfo.transformation(
            "coalesce(spark_catalog.default.t1.a, spark_catalog.default.t1.b) AS cond"));
    assertColumnDependsOnType(
        facet,
        "cond",
        FILE,
        T1_EXPECTED_NAME,
        "b",
        TransformationInfo.indirect(
            CONDITIONAL,
            "coalesce(spark_catalog.default.t1.a, spark_catalog.default.t1.b) AS cond"));
    assertCountDatasetDependencies(facet, 0);
  }

  @Test
  void simpleQueryWithNvl2Conditional() {
    createTable("t1", "a;int", "b;int", "c;int");
    OpenLineage.ColumnLineageDatasetFacet facet =
        getFacetForQuery(getSchemaFacet("cond;int"), "SELECT NVL2(a, b, c) AS cond FROM t1");
    assertCountColumnDependencies(facet, 3);
    assertColumnDependsOnType(
        facet,
        "cond",
        FILE,
        T1_EXPECTED_NAME,
        "a",
        TransformationInfo.indirect(
            CONDITIONAL,
            "(IF((spark_catalog.default.t1.a IS NOT NULL), spark_catalog.default.t1.b, spark_catalog.default.t1.c)) AS cond"));
    assertColumnDependsOnType(
        facet,
        "cond",
        FILE,
        T1_EXPECTED_NAME,
        "b",
        TransformationInfo.transformation(
            "(IF((spark_catalog.default.t1.a IS NOT NULL), spark_catalog.default.t1.b, spark_catalog.default.t1.c)) AS cond"));
    assertColumnDependsOnType(
        facet,
        "cond",
        FILE,
        T1_EXPECTED_NAME,
        "c",
        TransformationInfo.transformation(
            "(IF((spark_catalog.default.t1.a IS NOT NULL), spark_catalog.default.t1.b, spark_catalog.default.t1.c)) AS cond"));
    assertCountDatasetDependencies(facet, 0);
  }

  @Test
  void simpleQueryWithCoalesceConditional() {
    createTable("t1", "a;int", "b;int");
    OpenLineage.ColumnLineageDatasetFacet facet =
        getFacetForQuery(getSchemaFacet("cond;int"), "SELECT coalesce(a, b, 0) AS cond FROM t1");
    assertCountColumnDependencies(facet, 4);
    assertColumnDependsOnType(
        facet,
        "cond",
        FILE,
        T1_EXPECTED_NAME,
        "a",
        TransformationInfo.transformation(
            "coalesce(spark_catalog.default.t1.a, spark_catalog.default.t1.b, 0) AS cond"));
    assertColumnDependsOnType(
        facet,
        "cond",
        FILE,
        T1_EXPECTED_NAME,
        "a",
        TransformationInfo.indirect(
            CONDITIONAL,
            "coalesce(spark_catalog.default.t1.a, spark_catalog.default.t1.b, 0) AS cond"));
    assertColumnDependsOnType(
        facet,
        "cond",
        FILE,
        T1_EXPECTED_NAME,
        "b",
        TransformationInfo.transformation(
            "coalesce(spark_catalog.default.t1.a, spark_catalog.default.t1.b, 0) AS cond"));
    assertColumnDependsOnType(
        facet,
        "cond",
        FILE,
        T1_EXPECTED_NAME,
        "b",
        TransformationInfo.indirect(
            CONDITIONAL,
            "coalesce(spark_catalog.default.t1.a, spark_catalog.default.t1.b, 0) AS cond"));
    assertCountDatasetDependencies(facet, 0);
  }

  @Test
  void simpleQueryExplode() {
    createTable("t1", "a;string");
    OpenLineage.ColumnLineageDatasetFacet facet =
        getFacetForQuery(
            getSchemaFacet("a;string"),
            "SELECT a FROM (SELECT explode(split(a, ' ')) AS a FROM t1)");
    assertCountColumnDependencies(facet, 1);
    assertColumnDependsOnType(
        facet,
        "a",
        FILE,
        T1_EXPECTED_NAME,
        "a",
        TransformationInfo.transformation("explode(split(spark_catalog.default.t1.a, ' ', -1))"));
    assertCountDatasetDependencies(facet, 0);
  }

  @Test
  void simpleQueryRank() {
    createTable("t1", "a;string", "b;string", "c;int");
    OpenLineage.ColumnLineageDatasetFacet facet =
        getFacetForQuery(
            getSchemaFacet("a;string", "rank;int"),
            "SELECT a, RANK() OVER (PARTITION BY b ORDER BY c) as rank FROM t1;");
    assertCountColumnDependencies(facet, 3);
    assertColumnDependsOnType(
        facet, "a", FILE, T1_EXPECTED_NAME, "a", TransformationInfo.identity("a"));
    assertColumnDependsOnType(
        facet,
        "rank",
        FILE,
        T1_EXPECTED_NAME,
        "b",
        TransformationInfo.indirect(
            WINDOW,
            "RANK() OVER (PARTITION BY spark_catalog.default.t1.b ORDER BY spark_catalog.default.t1.c ASC NULLS FIRST ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS rank"));
    assertColumnDependsOnType(
        facet,
        "rank",
        FILE,
        T1_EXPECTED_NAME,
        "c",
        TransformationInfo.indirect(
            WINDOW,
            "RANK() OVER (PARTITION BY spark_catalog.default.t1.b ORDER BY spark_catalog.default.t1.c ASC NULLS FIRST ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS rank"));
    assertCountDatasetDependencies(facet, 0);
  }

  @Test
  void simpleQueryWindowedAggregate() {
    createTable("t1", "a;int", "b;string", "c;int");
    OpenLineage.ColumnLineageDatasetFacet facet =
        getFacetForQuery(
            getSchemaFacet("s;int"),
            "SELECT sum(a) OVER (PARTITION BY b ORDER BY c) AS s FROM t1;");
    assertCountColumnDependencies(facet, 3);
    assertColumnDependsOnType(
        facet,
        "s",
        FILE,
        T1_EXPECTED_NAME,
        "a",
        TransformationInfo.aggregation(
            "sum(spark_catalog.default.t1.a) OVER (PARTITION BY spark_catalog.default.t1.b ORDER BY spark_catalog.default.t1.c ASC NULLS FIRST RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS s"));
    assertColumnDependsOnType(
        facet,
        "s",
        FILE,
        T1_EXPECTED_NAME,
        "b",
        TransformationInfo.indirect(
            WINDOW,
            "sum(spark_catalog.default.t1.a) OVER (PARTITION BY spark_catalog.default.t1.b ORDER BY spark_catalog.default.t1.c ASC NULLS FIRST RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS s"));
    assertColumnDependsOnType(
        facet,
        "s",
        FILE,
        T1_EXPECTED_NAME,
        "c",
        TransformationInfo.indirect(
            WINDOW,
            "sum(spark_catalog.default.t1.a) OVER (PARTITION BY spark_catalog.default.t1.b ORDER BY spark_catalog.default.t1.c ASC NULLS FIRST RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS s"));
    assertCountDatasetDependencies(facet, 0);
  }

  @Test
  void simpleQueryWindowedTransformation() {
    createTable("t1", "a;int", "b;string", "c;int");
    OpenLineage.ColumnLineageDatasetFacet facet =
        getFacetForQuery(
            getSchemaFacet("l;int"),
            "SELECT LAG(a, 3, 0) OVER (PARTITION BY b ORDER BY c) AS l FROM t1;");
    assertCountColumnDependencies(facet, 3);
    assertColumnDependsOnType(
        facet,
        "l",
        FILE,
        T1_EXPECTED_NAME,
        "a",
        TransformationInfo.transformation(
            "lag(spark_catalog.default.t1.a, 3, 0) OVER (PARTITION BY spark_catalog.default.t1.b ORDER BY spark_catalog.default.t1.c ASC NULLS FIRST ROWS BETWEEN -3 FOLLOWING AND -3 FOLLOWING) AS l"));
    assertColumnDependsOnType(
        facet,
        "l",
        FILE,
        T1_EXPECTED_NAME,
        "b",
        TransformationInfo.indirect(
            WINDOW,
            "lag(spark_catalog.default.t1.a, 3, 0) OVER (PARTITION BY spark_catalog.default.t1.b ORDER BY spark_catalog.default.t1.c ASC NULLS FIRST ROWS BETWEEN -3 FOLLOWING AND -3 FOLLOWING) AS l"));
    assertColumnDependsOnType(
        facet,
        "l",
        FILE,
        T1_EXPECTED_NAME,
        "c",
        TransformationInfo.indirect(
            WINDOW,
            "lag(spark_catalog.default.t1.a, 3, 0) OVER (PARTITION BY spark_catalog.default.t1.b ORDER BY spark_catalog.default.t1.c ASC NULLS FIRST ROWS BETWEEN -3 FOLLOWING AND -3 FOLLOWING) AS l"));
    assertCountDatasetDependencies(facet, 0);
  }

  @Test
  void union() {
    createTable("t1", "a;int", "b;string");
    createTable("t2", "a;int", "c;string");
    OpenLineage.ColumnLineageDatasetFacet facet =
        getFacetForQuery(
            getSchemaFacet("a;int", "b;string"),
            "SELECT a, b, 'table1' as source\n"
                + "FROM t1\n"
                + "UNION ALL\n"
                + "SELECT a, c, 'table2' as source\n"
                + "FROM t2");
    assertCountColumnDependencies(facet, 4);
    assertColumnDependsOnType(
        facet, "a", FILE, T1_EXPECTED_NAME, "a", TransformationInfo.identity("a"));
    assertColumnDependsOnType(
        facet,
        "a",
        FILE,
        T2_EXPECTED_NAME,
        "a",
        TransformationInfo.identity("spark_catalog.default.t2.a"));
    assertColumnDependsOnType(
        facet, "b", FILE, T1_EXPECTED_NAME, "b", TransformationInfo.identity("b"));
    assertColumnDependsOnType(
        facet,
        "b",
        FILE,
        T2_EXPECTED_NAME,
        "c",
        TransformationInfo.identity("spark_catalog.default.t2.c"));
    assertCountDatasetDependencies(facet, 0);
  }

  @Test
  void simpleQueryMultipleJoinsToSameTable() {
    createTable("t1", "oder_id;int", "order_date;int", "shipped_date;int");
    createTable("t2", "date_id;int");
    OpenLineage.ColumnLineageDatasetFacet facet =
        getFacetForQuery(
            getSchemaFacet("oder_id;int", "order_date;int", "shipped_date;int"),
            "SELECT "
                + "t1.oder_id, "
                + "t2alias1.date_id as order_date, "
                + "t2alias2.date_id as shipped_date "
                + "FROM t1 "
                + "LEFT JOIN t2 t2alias1 ON t1.order_date = t2alias1.date_id "
                + "LEFT JOIN t2 t2alias2 ON t1.shipped_date = t2alias2.date_id "
                + "WHERE t1.order_date IS NOT NULL "
                + "AND t1.shipped_date IS NOT NULL");
    assertCountColumnDependencies(facet, 3);
    assertCountDatasetDependencies(facet, 8);
  }

  @Test
  void simpleDistinctQuery() {
    createTable("t1", "a;int");
    OpenLineage.ColumnLineageDatasetFacet facet =
        getFacetForQuery(getSchemaFacet("a;int"), "SELECT distinct a as a FROM t1");
    assertCountColumnDependencies(facet, 1);
    assertColumnDependsOnType(
        facet, "a", FILE, T1_EXPECTED_NAME, "a", TransformationInfo.identity("a"));
    assertCountDatasetDependencies(facet, 1);
    assertDatasetDependsOnType(
        facet,
        FILE,
        T1_EXPECTED_NAME,
        "a",
        TransformationInfo.indirect(GROUP_BY, "GROUP BY spark_catalog.default.t1.a"));
  }

  @Test
  void queryWithProgressivelyComplexTransformations() {
    createTable("t1", "a;string", "b;string", "c;string", "d;string", "e;string");
    OpenLineage.ColumnLineageDatasetFacet facet =
        getFacetForQuery(
            getSchemaFacet("col1;string", "col2;string", "col3;string", "col4;string"),
            "SELECT "
                + "a AS col1, "
                + "concat(b, e) AS col2, "
                + "upper(concat(c, e)) AS col3, "
                + "substring(upper(concat(d, e)), 1, 5) AS col4 "
                + "FROM t1");

    assertCountDatasetDependencies(facet, 0);
  }

  @NotNull
  private OpenLineage.ColumnLineageDatasetFacet getFacetForQuery(
      OpenLineage.SchemaDatasetFacet schemaFacet, String query) {
    return getFacetForQuery(schemaFacet, query, false);
  }

  @NotNull
  private OpenLineage.ColumnLineageDatasetFacet getFacetForQuery(
      OpenLineage.SchemaDatasetFacet schemaFacet, String query, Boolean isDistinct) {
    Dataset<Row> sql = spark.sql(query);
    if (isDistinct) {
      sql = sql.distinct();
    }
    return getColumnLineageDatasetFacet(schemaFacet, sql);
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
