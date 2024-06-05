/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.column;

import static io.openlineage.spark.agent.column.ColumnLevelLineageTestUtils.assertColumnDependsOn;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.Versions;
import io.openlineage.spark.agent.util.LastQueryExecutionSparkEventListener;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.SparkOpenLineageConfig;
import io.openlineage.spark3.agent.lifecycle.plan.column.ColumnLevelLineageUtils;
import java.util.Arrays;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.SparkSession$;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.QueryExecution;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

@Slf4j
@EnabledIfSystemProperty(named = "spark.version", matches = "(3.*)")
class ColumnLevelLineageHiveTest {

  private static final String FILE = "file";

  @SuppressWarnings("PMD")
  private static final String LOCAL_IP = "127.0.0.1";

  private static final String T1_EXPECTED_NAME = "column_non_v2/t1";
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
    SparkSession$.MODULE$.cleanupAnyExistingSession();
  }

  @AfterAll
  @SneakyThrows
  public static void afterAll() {
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
            .config("spark.driver.extraJavaOptions", "-Dderby.system.home=/tmp/col_non_v2/derby")
            .enableHiveSupport()
            .getOrCreate();

    context =
        OpenLineageContext.builder()
            .sparkSession(spark)
            .sparkContext(spark.sparkContext())
            .openLineage(new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI))
            .queryExecution(queryExecution)
            .meterRegistry(new SimpleMeterRegistry())
            .openLineageConfig(new SparkOpenLineageConfig())
            .build();

    FileSystem.get(spark.sparkContext().hadoopConfiguration())
        .delete(new Path("/tmp/column_non_v2/"), true);

    spark.sql("DROP TABLE IF EXISTS t1");
    spark.sql("DROP TABLE IF EXISTS t2");
    spark.sql("DROP TABLE IF EXISTS t");
  }

  @Test
  void testNonV2CreateTableAsSelect() {
    spark.sql("CREATE TABLE t1 (a int, b int) LOCATION '/tmp/column_non_v2/t1'");
    spark.sql("INSERT INTO t1 VALUES (1,2)");
    spark.sql("CREATE TABLE t2 LOCATION '/tmp/column_non_v2/t2' AS SELECT * FROM t1");

    LogicalPlan plan = LastQueryExecutionSparkEventListener.getLastExecutedLogicalPlan().get();
    when(queryExecution.optimizedPlan()).thenReturn(plan);
    OpenLineage.ColumnLineageDatasetFacet facet =
        ColumnLevelLineageUtils.buildColumnLineageDatasetFacet(event, context, schemaDatasetFacet)
            .get();

    assertColumnDependsOn(facet, "a", FILE, T1_EXPECTED_NAME, "a");
    assertColumnDependsOn(facet, "b", FILE, T1_EXPECTED_NAME, "b");
  }

  @Test
  void testNonV2CatalogInsertIntoTable() {
    spark.sql("CREATE TABLE t1 (a int, b int) LOCATION '/tmp/column_non_v2/t1'");
    spark.sql("INSERT INTO t1 VALUES (1,2)");
    spark.sql("CREATE TABLE t2 (a int, b int) LOCATION '/tmp/column_non_v2/t2'");
    spark.sql("INSERT INTO t2 SELECT * FROM t1");

    LogicalPlan plan = LastQueryExecutionSparkEventListener.getLastExecutedLogicalPlan().get();
    when(queryExecution.optimizedPlan()).thenReturn(plan);
    OpenLineage.ColumnLineageDatasetFacet facet =
        ColumnLevelLineageUtils.buildColumnLineageDatasetFacet(event, context, schemaDatasetFacet)
            .get();

    assertColumnDependsOn(facet, "a", FILE, T1_EXPECTED_NAME, "a");
    assertColumnDependsOn(facet, "b", FILE, T1_EXPECTED_NAME, "b");
  }

  @Test
  void testWhenSchemaIsNull() {
    when(queryExecution.optimizedPlan()).thenReturn(mock(LogicalPlan.class));
    assertDoesNotThrow(
        () -> ColumnLevelLineageUtils.buildColumnLineageDatasetFacet(event, context, null));
  }
  //
  //  @Test
  //  void testExpressionExtraction() {
  //
  //    OpenLineage.SchemaDatasetFacet schemaDatasetFacet1 =
  //        openLineage.newSchemaDatasetFacet(
  //            Arrays.asList(
  //                openLineage.newSchemaDatasetFacetFieldsBuilder().name("a").type("int").build(),
  //                openLineage.newSchemaDatasetFacetFieldsBuilder().name("b").type("int").build(),
  //                openLineage.newSchemaDatasetFacetFieldsBuilder().name("ab").type("int").build(),
  //
  // openLineage.newSchemaDatasetFacetFieldsBuilder().name("s").type("int").build()));
  //    spark.sql("CREATE TABLE t1 (a int, c int) LOCATION '/tmp/column_non_v2/t1'");
  //    spark.sql(
  //        "INSERT INTO t1 VALUES "
  //            + "(1,1), "
  //            + "(1,1), "
  //            + "(2,1), "
  //            + "(3,1), "
  //            + "(4,1), "
  //            + "(5,1), "
  //            + "(5,1)  ");
  //    spark.sql("CREATE TABLE t2 (a int, b string) LOCATION '/tmp/column_non_v2/t2'");
  //    spark.sql(
  //        "INSERT INTO t2 VALUES "
  //            + "(1,'a'), "
  //            + "(2,'b'), "
  //            + "(3,'c'), "
  //            + "(4,'d'), "
  //            + "(5,'e')  ");
  //
  //    Dataset<Row> sql =
  //        spark.sql(
  //            "select t3.a as a, "
  //                + "t2.b as b, "
  //                + "concat(t2.b, t3.a) as ab, "
  //                + "t3.s as s, "
  //                + "case when s > 1 then t3.a else substr(t3.a, 1) end as c "
  //                + "from (select a, sum(c) as s from t1 group by a, (c > 1)) t3 "
  //                + "join t2 on t2.a = t3.a "
  //                + "where t2.a = 1 "
  //                + "and t3.s > 1 "
  //                + "order by a");
  //
  //    LogicalPlan logicalPlan = sql.logicalPlan();
  //    Object dupa = sql.take(10);
  //
  //    LogicalPlan plan = LastQueryExecutionSparkEventListener.getLastExecutedLogicalPlan().get();
  //    when(queryExecution.optimizedPlan()).thenReturn(plan);
  //    OpenLineage.ColumnLineageDatasetFacet facet =
  //        ColumnLevelLineageUtils.buildColumnLineageDatasetFacet(event, context,
  // schemaDatasetFacet1)
  //            .get();
  //    String json = OpenLineageClientUtils.toJson(facet);
  //
  //    assertColumnDependsOn(facet, "a", FILE, T1_EXPECTED_NAME, "a");
  //    assertColumnDependsOn(facet, "b", FILE, T1_EXPECTED_NAME, "b");
  //  }
}
