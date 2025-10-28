/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.column;

import static io.openlineage.spark.agent.SparkTestUtils.SPARK_VERSION;

import io.openlineage.client.OpenLineage.ColumnLineageDatasetFacet;
import io.openlineage.spark.agent.MockServerUtils;
import java.util.Arrays;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.LongType$;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

/**
 * Integration tests for Set Operations (INTERSECT, EXCEPT) column lineage tracking.
 *
 * <p><b>IMPORTANT NOTE:</b> Spark's optimizer transforms INTERSECT and EXCEPT operations into other
 * nodes before OpenLineage processes them. Therefore, NO dedicated visitors are needed for these
 * operations:
 *
 * <ul>
 *   <li><b>INTERSECT DISTINCT</b> → {@code Distinct(Join(LeftSemi, ...))} - Handled by JoinVisitor
 *       + DistinctVisitor
 *   <li><b>INTERSECT ALL</b> → {@code Union + Aggregate + Generate + Filter} - Handled by
 *       UnionVisitor + AggregateVisitor + GenerateVisitor
 *   <li><b>EXCEPT DISTINCT</b> → {@code Distinct(Join(LeftAnti, ...))} - Handled by JoinVisitor +
 *       DistinctVisitor
 *   <li><b>EXCEPT ALL</b> → {@code Union + Aggregate + Generate + Filter} - Handled by existing
 *       visitors
 * </ul>
 *
 * <p>These tests verify that the existing column lineage visitors correctly handle the optimized
 * versions of set operations, ensuring complete column-level lineage is captured.
 *
 * @see io.openlineage.spark3.agent.lifecycle.plan.column.visitors.operator.JoinVisitor
 * @see io.openlineage.spark3.agent.lifecycle.plan.column.visitors.operator.DistinctVisitor
 * @see io.openlineage.spark3.agent.lifecycle.plan.column.visitors.operator.UnionVisitor
 * @see io.openlineage.spark3.agent.lifecycle.plan.column.visitors.operator.AggregateVisitor
 * @see io.openlineage.spark3.agent.lifecycle.plan.column.visitors.operator.GenerateVisitor
 */
@Slf4j
class SparkSetOperationsIntegrationTest extends SparkColumnLineageBaseTest {

  @Override
  protected int getMockServerPort() {
    return 1084;
  }

  @Override
  protected String getAppName() {
    return "SetOperationsIntegrationTest";
  }

  @Test
  @EnabledIfSystemProperty(named = SPARK_VERSION, matches = "([34].*)") // Spark >= 3.*
  void testIntersectColumnLineageDataFrameAPI() throws InterruptedException {
    String inputPath1 = "/tmp/test_data/intersect_input1";
    String inputPath2 = "/tmp/test_data/intersect_input2";
    String outputPath = "/tmp/test_data/intersect_output";

    // Create test data
    // Dataset 1: [(1, 10), (2, 20), (3, 30)]
    createTempDataset(3, 0).write().mode("overwrite").parquet(inputPath1);

    // Dataset 2: [(2, 20), (3, 30), (4, 40)]
    createTempDataset(3, 1).write().mode("overwrite").parquet(inputPath2);

    MockServerUtils.clearRequests(mockServer);

    // Execute INTERSECT using DataFrame API
    // Expected result: [(2, 20), (3, 30)] - rows in both datasets
    Dataset<Row> df1 = spark.read().parquet(inputPath1);
    Dataset<Row> df2 = spark.read().parquet(inputPath2);

    Dataset<Row> result = df1.intersect(df2);
    result.write().mode("overwrite").parquet(outputPath);

    spark.stop();

    logEmittedEvents();

    // Verify column lineage exists and is correct
    // INTERSECT DISTINCT → LeftSemi Join + Distinct, so columns a and b depend on both inputs
    ColumnLineageDatasetFacet columnLineage =
        verifyColumnLineageExists("intersect_output", "intersect_output");

    verifyColumnDependencies(columnLineage, "a", "a");
    verifyColumnDependencies(columnLineage, "b", "b");

    log.info("✅ INTERSECT (DataFrame API) column lineage test passed");
  }

  @Test
  @EnabledIfSystemProperty(named = SPARK_VERSION, matches = "([34].*)") // Spark >= 3.*
  void testIntersectColumnLineageSQL() {
    String inputPath1 = "/tmp/test_data/intersect_sql_input1";
    String inputPath2 = "/tmp/test_data/intersect_sql_input2";
    String outputPath = "/tmp/test_data/intersect_sql_output";

    // Create test data
    createTempDataset(3, 0).write().mode("overwrite").parquet(inputPath1);
    createTempDataset(3, 1).write().mode("overwrite").parquet(inputPath2);

    MockServerUtils.clearRequests(mockServer);

    // Execute INTERSECT using SQL
    Dataset<Row> df1 = spark.read().parquet(inputPath1);
    Dataset<Row> df2 = spark.read().parquet(inputPath2);

    df1.createOrReplaceTempView("intersect_table1");
    df2.createOrReplaceTempView("intersect_table2");

    Dataset<Row> result =
        spark.sql("SELECT * FROM intersect_table1 INTERSECT SELECT * FROM intersect_table2");
    result.write().mode("overwrite").parquet(outputPath);

    spark.stop();

    logEmittedEvents();

    ColumnLineageDatasetFacet columnLineage =
        verifyColumnLineageExists("intersect_sql_output", "intersect_sql_output");

    verifyColumnDependencies(columnLineage, "a", "a");
    verifyColumnDependencies(columnLineage, "b", "b");

    log.info("✅ INTERSECT (SQL) column lineage test passed");
  }

  @Test
  @EnabledIfSystemProperty(named = SPARK_VERSION, matches = "([34].*)") // Spark >= 3.*
  void testExceptColumnLineageDataFrameAPI() {
    String inputPath1 = "/tmp/test_data/except_input1";
    String inputPath2 = "/tmp/test_data/except_input2";
    String outputPath = "/tmp/test_data/except_output";

    // Create test data
    // Dataset 1: [(1, 10), (2, 20), (3, 30)]
    createTempDataset(3, 0).write().mode("overwrite").parquet(inputPath1);

    // Dataset 2: [(2, 20), (3, 30), (4, 40)]
    createTempDataset(3, 1).write().mode("overwrite").parquet(inputPath2);

    MockServerUtils.clearRequests(mockServer);

    // Execute EXCEPT using DataFrame API
    // Expected result: [(1, 10)] - rows in left but not in right
    Dataset<Row> df1 = spark.read().parquet(inputPath1);
    Dataset<Row> df2 = spark.read().parquet(inputPath2);

    Dataset<Row> result = df1.except(df2);
    result.write().mode("overwrite").parquet(outputPath);

    spark.stop();

    logEmittedEvents();

    ColumnLineageDatasetFacet columnLineage =
        verifyColumnLineageExists("except_output", "except_output");

    verifyColumnDependencies(columnLineage, "a", "a");
    verifyColumnDependencies(columnLineage, "b", "b");

    log.info("✅ EXCEPT (DataFrame API) column lineage test passed");
  }

  @Test
  @EnabledIfSystemProperty(named = SPARK_VERSION, matches = "([34].*)") // Spark >= 3.*
  void testExceptColumnLineageSQL() {
    String inputPath1 = "/tmp/test_data/except_sql_input1";
    String inputPath2 = "/tmp/test_data/except_sql_input2";
    String outputPath = "/tmp/test_data/except_sql_output";

    // Create test data
    createTempDataset(3, 0).write().mode("overwrite").parquet(inputPath1);
    createTempDataset(3, 1).write().mode("overwrite").parquet(inputPath2);

    MockServerUtils.clearRequests(mockServer);

    // Execute EXCEPT using SQL
    Dataset<Row> df1 = spark.read().parquet(inputPath1);
    Dataset<Row> df2 = spark.read().parquet(inputPath2);

    df1.createOrReplaceTempView("except_table1");
    df2.createOrReplaceTempView("except_table2");

    Dataset<Row> result =
        spark.sql("SELECT * FROM except_table1 EXCEPT SELECT * FROM except_table2");
    result.write().mode("overwrite").parquet(outputPath);

    spark.stop();

    logEmittedEvents();

    ColumnLineageDatasetFacet columnLineage =
        verifyColumnLineageExists("except_sql_output", "except_sql_output");

    verifyColumnDependencies(columnLineage, "a", "a");
    verifyColumnDependencies(columnLineage, "b", "b");

    log.info("✅ EXCEPT (SQL) column lineage test passed");
  }

  @Test
  @EnabledIfSystemProperty(named = SPARK_VERSION, matches = "([34].*)") // Spark >= 3.*
  void testIntersectAllColumnLineageSQL() {
    String inputPath1 = "/tmp/test_data/intersect_all_input1";
    String inputPath2 = "/tmp/test_data/intersect_all_input2";
    String outputPath = "/tmp/test_data/intersect_all_output";

    // Create test data with duplicates
    createTempDatasetWithDuplicates().write().mode("overwrite").parquet(inputPath1);
    createTempDatasetWithDuplicates().write().mode("overwrite").parquet(inputPath2);

    MockServerUtils.clearRequests(mockServer);

    // Execute INTERSECT ALL using SQL
    Dataset<Row> df1 = spark.read().parquet(inputPath1);
    Dataset<Row> df2 = spark.read().parquet(inputPath2);

    df1.createOrReplaceTempView("intersect_all_table1");
    df2.createOrReplaceTempView("intersect_all_table2");

    Dataset<Row> result =
        spark.sql(
            "SELECT * FROM intersect_all_table1 INTERSECT ALL SELECT * FROM intersect_all_table2");
    result.write().mode("overwrite").parquet(outputPath);

    spark.stop();

    logEmittedEvents();

    ColumnLineageDatasetFacet columnLineage =
        verifyColumnLineageExists("intersect_all_output", "intersect_all_output");

    verifyColumnDependencies(columnLineage, "a", "a");
    verifyColumnDependencies(columnLineage, "b", "b");

    log.info("✅ INTERSECT ALL (SQL) column lineage test passed");
  }

  @Test
  @EnabledIfSystemProperty(named = SPARK_VERSION, matches = "([34].*)") // Spark >= 3.*
  void testExceptAllColumnLineageSQL() {
    String inputPath1 = "/tmp/test_data/except_all_input1";
    String inputPath2 = "/tmp/test_data/except_all_input2";
    String outputPath = "/tmp/test_data/except_all_output";

    // Create test data with duplicates
    createTempDatasetWithDuplicates().write().mode("overwrite").parquet(inputPath1);
    createTempDatasetWithDuplicates().write().mode("overwrite").parquet(inputPath2);

    MockServerUtils.clearRequests(mockServer);

    // Execute EXCEPT ALL using SQL
    Dataset<Row> df1 = spark.read().parquet(inputPath1);
    Dataset<Row> df2 = spark.read().parquet(inputPath2);

    df1.createOrReplaceTempView("except_all_table1");
    df2.createOrReplaceTempView("except_all_table2");

    Dataset<Row> result =
        spark.sql("SELECT * FROM except_all_table1 EXCEPT ALL SELECT * FROM except_all_table2");
    result.write().mode("overwrite").parquet(outputPath);

    spark.stop();

    logEmittedEvents();

    ColumnLineageDatasetFacet columnLineage =
        verifyColumnLineageExists("except_all_output", "except_all_output");

    verifyColumnDependencies(columnLineage, "a", "a");
    verifyColumnDependencies(columnLineage, "b", "b");

    log.info("✅ EXCEPT ALL (SQL) column lineage test passed");
  }

  @Test
  @EnabledIfSystemProperty(named = SPARK_VERSION, matches = "([34].*)") // Spark >= 3.*
  void testComplexSetOperationColumnLineageSQL() {
    // Test more complex scenario using SQL: (A INTERSECT B) EXCEPT C
    String inputPathA = "/tmp/test_data/complex_set_sql_a";
    String inputPathB = "/tmp/test_data/complex_set_sql_b";
    String inputPathC = "/tmp/test_data/complex_set_sql_c";
    String outputPath = "/tmp/test_data/complex_set_sql_output";

    createTempDataset(5, 0).write().mode("overwrite").parquet(inputPathA);
    createTempDataset(5, 1).write().mode("overwrite").parquet(inputPathB);
    createTempDataset(5, 2).write().mode("overwrite").parquet(inputPathC);

    MockServerUtils.clearRequests(mockServer);

    Dataset<Row> dfA = spark.read().parquet(inputPathA);
    Dataset<Row> dfB = spark.read().parquet(inputPathB);
    Dataset<Row> dfC = spark.read().parquet(inputPathC);

    dfA.createOrReplaceTempView("complex_table_a");
    dfB.createOrReplaceTempView("complex_table_b");
    dfC.createOrReplaceTempView("complex_table_c");

    // (A INTERSECT B) EXCEPT C using SQL
    Dataset<Row> result =
        spark.sql(
            "(SELECT * FROM complex_table_a INTERSECT SELECT * FROM complex_table_b) "
                + "EXCEPT SELECT * FROM complex_table_c");
    result.write().mode("overwrite").parquet(outputPath);

    spark.stop();

    logEmittedEvents();

    // Verify column lineage tracks all operations
    ColumnLineageDatasetFacet columnLineage =
        verifyColumnLineageExists("complex_set_sql_output", "complex_set_sql_output");

    // Nested operations should still produce correct lineage
    verifyColumnDependencies(columnLineage, "a", "a");
    verifyColumnDependencies(columnLineage, "b", "b");

    log.info("✅ Complex set operation (SQL) column lineage test passed");
  }

  /**
   * Helper method to create test dataset with duplicate values for INTERSECT ALL / EXCEPT ALL
   * tests.
   */
  private Dataset<Row> createTempDatasetWithDuplicates() {
    List<Row> rowList =
        Arrays.asList(
            RowFactory.create(1L, 10L),
            RowFactory.create(2L, 20L),
            RowFactory.create(2L, 20L), // duplicate
            RowFactory.create(3L, 30L));

    return spark
        .createDataFrame(
            rowList,
            new StructType(
                new StructField[] {
                  new StructField("a", LongType$.MODULE$, false, Metadata.empty()),
                  new StructField("b", LongType$.MODULE$, false, Metadata.empty())
                }))
        .repartition(1);
  }
}
