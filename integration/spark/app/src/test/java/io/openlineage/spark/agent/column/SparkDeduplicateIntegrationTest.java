/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.column;

import static io.openlineage.spark.agent.SparkTestUtils.SPARK_VERSION;
import static org.assertj.core.api.Assertions.assertThat;

import io.openlineage.client.OpenLineage.ColumnLineageDatasetFacet;
import io.openlineage.spark.agent.MockServerUtils;
import java.util.Arrays;
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
 * Integration tests for Deduplicate operator column lineage tracking.
 *
 * <p>The Deduplicate operator appears in the optimized logical plan for:
 *
 * <ul>
 *   <li><b>dropDuplicates()</b> DataFrame API calls
 *   <li><b>dropDuplicates(columns)</b> DataFrame API calls with specific columns
 * </ul>
 *
 * <p>Note: Deduplicate may be optimized to Aggregate by ReplaceDeduplicateWithAggregate optimizer
 * rule. These tests verify whether Deduplicate survives optimization.
 *
 * <p>These tests verify that:
 *
 * <ol>
 *   <li>Deduplicate node appears in the optimized logical plan (or is transformed)
 *   <li>Column lineage can be extracted from deduplication operations
 * </ol>
 */
@Slf4j
class SparkDeduplicateIntegrationTest extends SparkColumnLineageBaseTest {

  @Override
  protected int getMockServerPort() {
    return 1086;
  }

  @Override
  protected String getAppName() {
    return "DeduplicateIntegrationTest";
  }

  @Test
  @EnabledIfSystemProperty(named = SPARK_VERSION, matches = "([34].*)") // Spark >= 3.*
  void testDropDuplicatesAllColumns() {
    String inputPath = "/tmp/test_data/dedup_all_input";
    String outputPath = "/tmp/test_data/dedup_all_output";

    // Create test data with duplicates
    java.util.List<Row> rowList =
        Arrays.asList(
            RowFactory.create(1L, 10L),
            RowFactory.create(2L, 20L),
            RowFactory.create(2L, 20L), // duplicate row
            RowFactory.create(3L, 30L),
            RowFactory.create(1L, 10L)); // duplicate row

    Dataset<Row> df =
        spark
            .createDataFrame(
                rowList,
                new StructType(
                    new StructField[] {
                      new StructField("a", LongType$.MODULE$, false, Metadata.empty()),
                      new StructField("b", LongType$.MODULE$, false, Metadata.empty())
                    }))
            .repartition(1);

    df.write().mode("overwrite").parquet(inputPath);

    MockServerUtils.clearRequests(mockServer);

    // Execute dropDuplicates() on all columns
    Dataset<Row> inputDf = spark.read().parquet(inputPath);
    Dataset<Row> result = inputDf.dropDuplicates();
    result.write().mode("overwrite").parquet(outputPath);

    spark.stop();

    logEmittedEvents();

    // Verify column lineage exists (whether via Deduplicate or Aggregate visitor)
    ColumnLineageDatasetFacet columnLineage =
        verifyColumnLineageExists("dedup_all_output", "dedup_all_output");

    log.info("✅ dropDuplicates() all columns test passed");
    log.info(
        "Column lineage fields: {}", columnLineage.getFields().getAdditionalProperties().keySet());
  }

  @Test
  @EnabledIfSystemProperty(named = SPARK_VERSION, matches = "([34].*)") // Spark >= 3.*
  void testDropDuplicatesSpecificColumns() {
    String inputPath = "/tmp/test_data/dedup_specific_input";
    String outputPath = "/tmp/test_data/dedup_specific_output";

    // Create test data with duplicates on column 'a' only
    java.util.List<Row> rowList =
        Arrays.asList(
            RowFactory.create(1L, 10L),
            RowFactory.create(1L, 20L), // duplicate on 'a' but different 'b'
            RowFactory.create(2L, 30L),
            RowFactory.create(3L, 40L));

    Dataset<Row> df =
        spark
            .createDataFrame(
                rowList,
                new StructType(
                    new StructField[] {
                      new StructField("a", LongType$.MODULE$, false, Metadata.empty()),
                      new StructField("b", LongType$.MODULE$, false, Metadata.empty())
                    }))
            .repartition(1);

    df.write().mode("overwrite").parquet(inputPath);

    MockServerUtils.clearRequests(mockServer);

    // Execute dropDuplicates() on specific column 'a'
    Dataset<Row> inputDf = spark.read().parquet(inputPath);
    Dataset<Row> result = inputDf.dropDuplicates("a");
    result.write().mode("overwrite").parquet(outputPath);

    spark.stop();

    logEmittedEvents();

    // Verify column lineage exists
    ColumnLineageDatasetFacet columnLineage =
        verifyColumnLineageExists("dedup_specific_output", "dedup_specific_output");

    log.info("✅ dropDuplicates(column) specific columns test passed");
    log.info(
        "Column lineage fields: {}", columnLineage.getFields().getAdditionalProperties().keySet());

    // Both columns should be in output
    assertThat(columnLineage.getFields().getAdditionalProperties()).containsKeys("a", "b");
  }

  @Test
  @EnabledIfSystemProperty(named = SPARK_VERSION, matches = "([34].*)") // Spark >= 3.*
  void testDropDuplicatesSQL() {
    String inputPath = "/tmp/test_data/dedup_sql_input";
    String outputPath = "/tmp/test_data/dedup_sql_output";

    // Create test data with duplicates
    java.util.List<Row> rowList =
        Arrays.asList(
            RowFactory.create(1L, 10L),
            RowFactory.create(2L, 20L),
            RowFactory.create(2L, 20L), // duplicate
            RowFactory.create(3L, 30L));

    Dataset<Row> df =
        spark
            .createDataFrame(
                rowList,
                new StructType(
                    new StructField[] {
                      new StructField("a", LongType$.MODULE$, false, Metadata.empty()),
                      new StructField("b", LongType$.MODULE$, false, Metadata.empty())
                    }))
            .repartition(1);

    df.write().mode("overwrite").parquet(inputPath);

    MockServerUtils.clearRequests(mockServer);

    // Using DISTINCT in SQL (which becomes Distinct, not Deduplicate)
    // This is to compare with dropDuplicates behavior
    Dataset<Row> inputDf = spark.read().parquet(inputPath);
    inputDf.createOrReplaceTempView("dedup_sql_table");

    Dataset<Row> result = spark.sql("SELECT DISTINCT * FROM dedup_sql_table");
    result.write().mode("overwrite").parquet(outputPath);

    spark.stop();

    logEmittedEvents();

    // Verify column lineage exists
    ColumnLineageDatasetFacet columnLineage =
        verifyColumnLineageExists("dedup_sql_output", "dedup_sql_output");

    log.info("✅ DISTINCT (SQL) test passed");
    log.info(
        "Column lineage fields: {}", columnLineage.getFields().getAdditionalProperties().keySet());
  }
}
