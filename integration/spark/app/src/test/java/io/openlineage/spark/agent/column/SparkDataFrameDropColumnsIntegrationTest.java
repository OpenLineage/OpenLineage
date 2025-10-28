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
import org.apache.spark.sql.types.StringType$;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

/**
 * Integration tests for DataFrameDropColumns operator column lineage tracking.
 *
 * <p>The DataFrameDropColumns operator appears when using the DataFrame.drop(columns) API. This
 * test verifies whether this node survives optimization or is transformed into other operators
 * (likely Project).
 *
 * <p>These tests verify that:
 *
 * <ol>
 *   <li>DataFrameDropColumns node appears in the optimized logical plan (or is transformed to
 *       Project)
 *   <li>Column lineage correctly tracks which columns are dropped
 * </ol>
 */
@Slf4j
class SparkDataFrameDropColumnsIntegrationTest extends SparkColumnLineageBaseTest {

  @Override
  protected int getMockServerPort() {
    return 1088;
  }

  @Override
  protected String getAppName() {
    return "DataFrameDropColumnsIntegrationTest";
  }

  @Test
  @EnabledIfSystemProperty(named = SPARK_VERSION, matches = "([34].*)") // Spark >= 3.*
  void testDropSingleColumn() {
    String inputPath = "/tmp/test_data/drop_single_input";
    String outputPath = "/tmp/test_data/drop_single_output";

    // Create test data with 4 columns
    java.util.List<Row> rowList =
        Arrays.asList(
            RowFactory.create(1L, "name1", 100L, "extra1"),
            RowFactory.create(2L, "name2", 200L, "extra2"),
            RowFactory.create(3L, "name3", 300L, "extra3"));

    Dataset<Row> df =
        spark
            .createDataFrame(
                rowList,
                new StructType(
                    new StructField[] {
                      new StructField("id", LongType$.MODULE$, false, Metadata.empty()),
                      new StructField("name", StringType$.MODULE$, false, Metadata.empty()),
                      new StructField("value", LongType$.MODULE$, false, Metadata.empty()),
                      new StructField("extra", StringType$.MODULE$, false, Metadata.empty())
                    }))
            .repartition(1);

    df.write().mode("overwrite").parquet(inputPath);

    MockServerUtils.clearRequests(mockServer);

    // Execute drop() to remove 'extra' column
    Dataset<Row> inputDf = spark.read().parquet(inputPath);
    Dataset<Row> result = inputDf.drop("extra");
    result.write().mode("overwrite").parquet(outputPath);

    spark.stop();

    logEmittedEvents();

    // Verify column lineage exists
    ColumnLineageDatasetFacet columnLineage =
        verifyColumnLineageExists("drop_single_output", "drop_single_output");

    log.info("✅ drop(column) single column test passed");
    log.info(
        "Column lineage fields: {}", columnLineage.getFields().getAdditionalProperties().keySet());

    // Output should have id, name, value (but not extra)
    assertThat(columnLineage.getFields().getAdditionalProperties())
        .containsKeys("id", "name", "value")
        .doesNotContainKey("extra");
  }

  @Test
  @EnabledIfSystemProperty(named = SPARK_VERSION, matches = "([34].*)") // Spark >= 3.*
  void testDropMultipleColumns() {
    String inputPath = "/tmp/test_data/drop_multiple_input";
    String outputPath = "/tmp/test_data/drop_multiple_output";

    // Create test data with 5 columns
    java.util.List<Row> rowList =
        Arrays.asList(
            RowFactory.create(1L, "name1", 100L, "extra1", "extra2"),
            RowFactory.create(2L, "name2", 200L, "extra3", "extra4"));

    Dataset<Row> df =
        spark
            .createDataFrame(
                rowList,
                new StructType(
                    new StructField[] {
                      new StructField("id", LongType$.MODULE$, false, Metadata.empty()),
                      new StructField("name", StringType$.MODULE$, false, Metadata.empty()),
                      new StructField("value", LongType$.MODULE$, false, Metadata.empty()),
                      new StructField("extra1", StringType$.MODULE$, false, Metadata.empty()),
                      new StructField("extra2", StringType$.MODULE$, false, Metadata.empty())
                    }))
            .repartition(1);

    df.write().mode("overwrite").parquet(inputPath);

    MockServerUtils.clearRequests(mockServer);

    // Execute drop() to remove multiple columns
    Dataset<Row> inputDf = spark.read().parquet(inputPath);
    Dataset<Row> result = inputDf.drop("extra1", "extra2");
    result.write().mode("overwrite").parquet(outputPath);

    spark.stop();

    logEmittedEvents();

    // Verify column lineage exists
    ColumnLineageDatasetFacet columnLineage =
        verifyColumnLineageExists("drop_multiple_output", "drop_multiple_output");

    log.info("✅ drop(columns...) multiple columns test passed");
    log.info(
        "Column lineage fields: {}", columnLineage.getFields().getAdditionalProperties().keySet());

    // Output should have id, name, value (but not extra1, extra2)
    assertThat(columnLineage.getFields().getAdditionalProperties())
        .containsKeys("id", "name", "value")
        .doesNotContainKeys("extra1", "extra2");
  }
}
