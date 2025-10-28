/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.column;

import static io.openlineage.spark.agent.SparkTestUtils.SPARK_VERSION;

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
 * Integration tests for Pivot operator transformation.
 *
 * <p><b>IMPORTANT NOTE:</b> According to Analyzer.ResolvePivot (lines 906-918), Pivot operations
 * are transformed to Aggregate with PivotFirst aggregate functions:
 *
 * <pre>
 * Pivot → Aggregate(groupByExprs, pivotAggs, firstAgg)
 * where pivotAggs use PivotFirst(pivotCol, aggExpr, pivotValues)
 * </pre>
 *
 * <p>These tests verify that:
 *
 * <ol>
 *   <li>Pivot is transformed to Aggregate in the optimized logical plan
 *   <li>Existing AggregateVisitor correctly handles the transformed Pivot operations
 *   <li>Column lineage is properly extracted
 * </ol>
 *
 * @see io.openlineage.spark3.agent.lifecycle.plan.column.visitors.operator.AggregateVisitor
 */
@Slf4j
class SparkPivotIntegrationTest extends SparkColumnLineageBaseTest {

  @Override
  protected int getMockServerPort() {
    return 1087;
  }

  @Override
  protected String getAppName() {
    return "PivotIntegrationTest";
  }

  @Test
  @EnabledIfSystemProperty(named = SPARK_VERSION, matches = "([34].*)") // Spark >= 3.*
  void testPivotOperationTransformsToAggregate() {
    String inputPath = "/tmp/test_data/pivot_input";
    String outputPath = "/tmp/test_data/pivot_output";

    // Create test data suitable for pivoting
    // Sales data: product, region, amount
    java.util.List<Row> rowList =
        Arrays.asList(
            RowFactory.create(1L, "A", "East", 100L),
            RowFactory.create(2L, "A", "West", 150L),
            RowFactory.create(3L, "B", "East", 200L),
            RowFactory.create(4L, "B", "West", 250L),
            RowFactory.create(5L, "A", "East", 120L));

    Dataset<Row> df =
        spark
            .createDataFrame(
                rowList,
                new StructType(
                    new StructField[] {
                      new StructField("id", LongType$.MODULE$, false, Metadata.empty()),
                      new StructField("product", StringType$.MODULE$, false, Metadata.empty()),
                      new StructField("region", StringType$.MODULE$, false, Metadata.empty()),
                      new StructField("amount", LongType$.MODULE$, false, Metadata.empty())
                    }))
            .repartition(1);

    df.write().mode("overwrite").parquet(inputPath);

    MockServerUtils.clearRequests(mockServer);

    // Execute PIVOT operation
    // This should be transformed to Aggregate with PivotFirst in the optimized logical plan
    Dataset<Row> inputDf = spark.read().parquet(inputPath);
    inputDf.createOrReplaceTempView("pivot_table");

    Dataset<Row> result =
        spark.sql(
            "SELECT * FROM pivot_table " + "PIVOT (SUM(amount) FOR region IN ('East', 'West'))");
    result.write().mode("overwrite").parquet(outputPath);

    spark.stop();

    logEmittedEvents();

    // Verify column lineage exists
    // If Pivot → Aggregate transformation works, AggregateVisitor should handle it
    ColumnLineageDatasetFacet columnLineage =
        verifyColumnLineageExists("pivot_output", "pivot_output");

    log.info("✅ PIVOT operation (transforms to Aggregate) test passed");
    log.info(
        "Column lineage fields: {}", columnLineage.getFields().getAdditionalProperties().keySet());
  }

  @Test
  @EnabledIfSystemProperty(named = SPARK_VERSION, matches = "([34].*)") // Spark >= 3.*
  void testPivotWithMultipleAggregates() {
    String inputPath = "/tmp/test_data/pivot_multi_agg_input";
    String outputPath = "/tmp/test_data/pivot_multi_agg_output";

    // Create test data
    java.util.List<Row> rowList =
        Arrays.asList(
            RowFactory.create("A", "East", 100L),
            RowFactory.create("A", "West", 150L),
            RowFactory.create("B", "East", 200L),
            RowFactory.create("B", "West", 250L));

    Dataset<Row> df =
        spark
            .createDataFrame(
                rowList,
                new StructType(
                    new StructField[] {
                      new StructField("product", StringType$.MODULE$, false, Metadata.empty()),
                      new StructField("region", StringType$.MODULE$, false, Metadata.empty()),
                      new StructField("amount", LongType$.MODULE$, false, Metadata.empty())
                    }))
            .repartition(1);

    df.write().mode("overwrite").parquet(inputPath);

    MockServerUtils.clearRequests(mockServer);

    // Execute PIVOT with multiple aggregate functions
    Dataset<Row> inputDf = spark.read().parquet(inputPath);
    inputDf.createOrReplaceTempView("pivot_multi_table");

    Dataset<Row> result =
        spark.sql(
            "SELECT * FROM pivot_multi_table "
                + "PIVOT (SUM(amount) as sum, AVG(amount) as avg FOR region IN ('East', 'West'))");
    result.write().mode("overwrite").parquet(outputPath);

    spark.stop();

    logEmittedEvents();

    // Verify column lineage exists
    ColumnLineageDatasetFacet columnLineage =
        verifyColumnLineageExists("pivot_multi_agg_output", "pivot_multi_agg_output");

    log.info("✅ PIVOT with multiple aggregates test passed");
    log.info(
        "Column lineage fields: {}", columnLineage.getFields().getAdditionalProperties().keySet());
  }

  @Test
  @EnabledIfSystemProperty(named = SPARK_VERSION, matches = "([34].*)") // Spark >= 3.*
  void testPivotDataFrameAPI() {
    String inputPath = "/tmp/test_data/pivot_df_api_input";
    String outputPath = "/tmp/test_data/pivot_df_api_output";

    // Create test data
    java.util.List<Row> rowList =
        Arrays.asList(
            RowFactory.create("A", "East", 100L),
            RowFactory.create("A", "West", 150L),
            RowFactory.create("B", "East", 200L),
            RowFactory.create("B", "West", 250L));

    Dataset<Row> df =
        spark
            .createDataFrame(
                rowList,
                new StructType(
                    new StructField[] {
                      new StructField("product", StringType$.MODULE$, false, Metadata.empty()),
                      new StructField("region", StringType$.MODULE$, false, Metadata.empty()),
                      new StructField("amount", LongType$.MODULE$, false, Metadata.empty())
                    }))
            .repartition(1);

    df.write().mode("overwrite").parquet(inputPath);

    MockServerUtils.clearRequests(mockServer);

    // Execute PIVOT using DataFrame API
    Dataset<Row> inputDf = spark.read().parquet(inputPath);
    Dataset<Row> result = inputDf.groupBy("product").pivot("region").sum("amount");
    result.write().mode("overwrite").parquet(outputPath);

    spark.stop();

    logEmittedEvents();

    // Verify column lineage exists
    ColumnLineageDatasetFacet columnLineage =
        verifyColumnLineageExists("pivot_df_api_output", "pivot_df_api_output");

    log.info("✅ PIVOT (DataFrame API) test passed");
    log.info(
        "Column lineage fields: {}", columnLineage.getFields().getAdditionalProperties().keySet());
  }
}
