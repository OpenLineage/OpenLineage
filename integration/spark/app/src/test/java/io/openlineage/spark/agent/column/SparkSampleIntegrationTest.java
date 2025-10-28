/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.column;

import static io.openlineage.spark.agent.SparkTestUtils.SPARK_VERSION;

import io.openlineage.client.OpenLineage.ColumnLineageDatasetFacet;
import io.openlineage.spark.agent.MockServerUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

/**
 * Integration tests for Sample operator column lineage tracking.
 *
 * <p>The Sample operator appears in the optimized logical plan for:
 *
 * <ul>
 *   <li><b>sample()</b> DataFrame API calls
 *   <li><b>sample(withReplacement, fraction)</b> DataFrame API calls
 * </ul>
 *
 * <p>These tests verify that:
 *
 * <ol>
 *   <li>Sample node appears in the optimized logical plan (not optimized away)
 *   <li>Column lineage can be extracted from sampling operations
 * </ol>
 */
@Slf4j
class SparkSampleIntegrationTest extends SparkColumnLineageBaseTest {

  @Override
  protected int getMockServerPort() {
    return 1089;
  }

  @Override
  protected String getAppName() {
    return "SampleIntegrationTest";
  }

  @Test
  @EnabledIfSystemProperty(named = SPARK_VERSION, matches = "([34].*)") // Spark >= 3.*
  void testSampleWithoutReplacement() {
    String inputPath = "/tmp/test_data/sample_no_replace_input";
    String outputPath = "/tmp/test_data/sample_no_replace_output";

    // Create test data with enough rows to make sampling meaningful
    createTempDataset(100, 0).write().mode("overwrite").parquet(inputPath);

    MockServerUtils.clearRequests(mockServer);

    // Execute sample() without replacement
    Dataset<Row> inputDf = spark.read().parquet(inputPath);
    Dataset<Row> result = inputDf.sample(false, 0.5, 42L); // 50% sample, fixed seed
    result.write().mode("overwrite").parquet(outputPath);

    spark.stop();

    logEmittedEvents();

    // Verify column lineage exists and is correct
    ColumnLineageDatasetFacet columnLineage =
        verifyColumnLineageExists("sample_no_replace_output", "sample_no_replace_output");

    // Sample should pass through all columns with identity transformation
    verifyColumnDependencies(columnLineage, "a", "a");
    verifyColumnDependencies(columnLineage, "b", "b");

    log.info("✅ sample(withReplacement=false) test passed");
    log.info(
        "Column lineage fields: {}", columnLineage.getFields().getAdditionalProperties().keySet());
  }

  @Test
  @EnabledIfSystemProperty(named = SPARK_VERSION, matches = "([34].*)") // Spark >= 3.*
  void testSampleWithReplacement() {
    String inputPath = "/tmp/test_data/sample_replace_input";
    String outputPath = "/tmp/test_data/sample_replace_output";

    // Create test data
    createTempDataset(100, 0).write().mode("overwrite").parquet(inputPath);

    MockServerUtils.clearRequests(mockServer);

    // Execute sample() with replacement
    Dataset<Row> inputDf = spark.read().parquet(inputPath);
    Dataset<Row> result = inputDf.sample(true, 0.5, 42L); // 50% sample with replacement
    result.write().mode("overwrite").parquet(outputPath);

    spark.stop();

    logEmittedEvents();

    // Verify column lineage exists and is correct
    ColumnLineageDatasetFacet columnLineage =
        verifyColumnLineageExists("sample_replace_output", "sample_replace_output");

    // Sample should pass through all columns with identity transformation
    verifyColumnDependencies(columnLineage, "a", "a");
    verifyColumnDependencies(columnLineage, "b", "b");

    log.info("✅ sample(withReplacement=true) test passed");
    log.info(
        "Column lineage fields: {}", columnLineage.getFields().getAdditionalProperties().keySet());
  }

  @Test
  @EnabledIfSystemProperty(named = SPARK_VERSION, matches = "([34].*)") // Spark >= 3.*
  void testSampleSQL() {
    String inputPath = "/tmp/test_data/sample_sql_input";
    String outputPath = "/tmp/test_data/sample_sql_output";

    // Create test data
    createTempDataset(100, 0).write().mode("overwrite").parquet(inputPath);

    MockServerUtils.clearRequests(mockServer);

    // Execute TABLESAMPLE using SQL
    Dataset<Row> inputDf = spark.read().parquet(inputPath);
    inputDf.createOrReplaceTempView("sample_table");

    Dataset<Row> result = spark.sql("SELECT * FROM sample_table TABLESAMPLE (50 PERCENT)");
    result.write().mode("overwrite").parquet(outputPath);

    spark.stop();

    logEmittedEvents();

    // Verify column lineage exists and is correct
    ColumnLineageDatasetFacet columnLineage =
        verifyColumnLineageExists("sample_sql_output", "sample_sql_output");

    // Sample should pass through all columns with identity transformation
    verifyColumnDependencies(columnLineage, "a", "a");
    verifyColumnDependencies(columnLineage, "b", "b");

    log.info("✅ TABLESAMPLE (SQL) test passed");
    log.info(
        "Column lineage fields: {}", columnLineage.getFields().getAdditionalProperties().keySet());
  }
}
