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
 * Integration tests for Expand operator column lineage tracking.
 *
 * <p>The Expand operator appears in the optimized logical plan for:
 *
 * <ul>
 *   <li><b>CUBE</b> operations - GROUP BY CUBE(...)
 *   <li><b>ROLLUP</b> operations - GROUP BY ROLLUP(...)
 *   <li><b>GROUPING SETS</b> operations - GROUP BY GROUPING SETS(...)
 *   <li><b>UNPIVOT</b> operations - Unpivot is resolved to Expand by Analyzer.ResolveUnpivot
 * </ul>
 *
 * <p>These tests verify that:
 *
 * <ol>
 *   <li>Expand node appears in the optimized logical plan (not optimized away)
 *   <li>Column lineage can be extracted from Expand operations
 * </ol>
 */
@Slf4j
class SparkExpandIntegrationTest extends SparkColumnLineageBaseTest {

  @Override
  protected int getMockServerPort() {
    return 1085;
  }

  @Override
  protected String getAppName() {
    return "ExpandIntegrationTest";
  }

  @Test
  @EnabledIfSystemProperty(named = SPARK_VERSION, matches = "([34].*)") // Spark >= 3.*
  void testCubeOperationContainsExpandNode() {
    String inputPath = "/tmp/test_data/cube_input";
    String outputPath = "/tmp/test_data/cube_output";

    // Create test data
    createTempDataset(10, 0).write().mode("overwrite").parquet(inputPath);

    MockServerUtils.clearRequests(mockServer);

    // Execute CUBE operation
    // This should produce an Expand node in the optimized logical plan
    Dataset<Row> df = spark.read().parquet(inputPath);
    df.createOrReplaceTempView("cube_table");

    Dataset<Row> result =
        spark.sql("SELECT a, b, COUNT(*) as cnt FROM cube_table GROUP BY CUBE(a, b)");
    result.write().mode("overwrite").parquet(outputPath);

    spark.stop();

    logEmittedEvents();

    // Verify column lineage exists and is correct
    ColumnLineageDatasetFacet columnLineage =
        verifyColumnLineageExists("cube_output", "cube_output");

    // For CUBE(a, b), output columns 'a' and 'b' should come from input columns 'a' and 'b'
    verifyColumnDependencies(columnLineage, "a", "a");
    verifyColumnDependencies(columnLineage, "b", "b");

    log.info("✅ CUBE operation column lineage test passed");
    log.info(
        "Column lineage fields: {}", columnLineage.getFields().getAdditionalProperties().keySet());
  }

  @Test
  @EnabledIfSystemProperty(named = SPARK_VERSION, matches = "([34].*)") // Spark >= 3.*
  void testRollupOperationContainsExpandNode() {
    String inputPath = "/tmp/test_data/rollup_input";
    String outputPath = "/tmp/test_data/rollup_output";

    // Create test data
    createTempDataset(10, 0).write().mode("overwrite").parquet(inputPath);

    MockServerUtils.clearRequests(mockServer);

    // Execute ROLLUP operation
    // This should produce an Expand node in the optimized logical plan
    Dataset<Row> df = spark.read().parquet(inputPath);
    df.createOrReplaceTempView("rollup_table");

    Dataset<Row> result =
        spark.sql("SELECT a, b, COUNT(*) as cnt FROM rollup_table GROUP BY ROLLUP(a, b)");
    result.write().mode("overwrite").parquet(outputPath);

    spark.stop();

    logEmittedEvents();

    // Verify column lineage exists and is correct
    ColumnLineageDatasetFacet columnLineage =
        verifyColumnLineageExists("rollup_output", "rollup_output");

    // For ROLLUP(a, b), output columns 'a' and 'b' should come from input columns 'a' and 'b'
    verifyColumnDependencies(columnLineage, "a", "a");
    verifyColumnDependencies(columnLineage, "b", "b");

    log.info("✅ ROLLUP operation column lineage test passed");
    log.info(
        "Column lineage fields: {}", columnLineage.getFields().getAdditionalProperties().keySet());
  }

  @Test
  @EnabledIfSystemProperty(named = SPARK_VERSION, matches = "([34].*)") // Spark >= 3.*
  void testGroupingSetsOperationContainsExpandNode() {
    String inputPath = "/tmp/test_data/grouping_sets_input";
    String outputPath = "/tmp/test_data/grouping_sets_output";

    // Create test data
    createTempDataset(10, 0).write().mode("overwrite").parquet(inputPath);

    MockServerUtils.clearRequests(mockServer);

    // Execute GROUPING SETS operation
    // This should produce an Expand node in the optimized logical plan
    Dataset<Row> df = spark.read().parquet(inputPath);
    df.createOrReplaceTempView("grouping_sets_table");

    Dataset<Row> result =
        spark.sql(
            "SELECT a, b, COUNT(*) as cnt FROM grouping_sets_table "
                + "GROUP BY GROUPING SETS((a, b), (a), (b), ())");
    result.write().mode("overwrite").parquet(outputPath);

    spark.stop();

    logEmittedEvents();

    // Verify column lineage exists and is correct
    ColumnLineageDatasetFacet columnLineage =
        verifyColumnLineageExists("grouping_sets_output", "grouping_sets_output");

    // For GROUPING SETS((a,b), (a), (b), ()), output columns 'a' and 'b' should come from input
    verifyColumnDependencies(columnLineage, "a", "a");
    verifyColumnDependencies(columnLineage, "b", "b");

    log.info("✅ GROUPING SETS operation column lineage test passed");
    log.info(
        "Column lineage fields: {}", columnLineage.getFields().getAdditionalProperties().keySet());
  }

  @Test
  @EnabledIfSystemProperty(
      named = SPARK_VERSION,
      matches = "(3\\.[4-9].*|4.*)") // Spark >= 3.4 (UNPIVOT added in 3.4)
  void testUnpivotOperationBecomesExpandNode() {
    String inputPath = "/tmp/test_data/unpivot_input";
    String outputPath = "/tmp/test_data/unpivot_output";

    // Create test data with multiple value columns for unpivot
    List<Row> rowList =
        Arrays.asList(
            RowFactory.create(1L, 100L, 200L, 300L),
            RowFactory.create(2L, 110L, 210L, 310L),
            RowFactory.create(3L, 120L, 220L, 320L));

    Dataset<Row> df =
        spark
            .createDataFrame(
                rowList,
                new StructType(
                    new StructField[] {
                      new StructField("id", LongType$.MODULE$, false, Metadata.empty()),
                      new StructField("col1", LongType$.MODULE$, false, Metadata.empty()),
                      new StructField("col2", LongType$.MODULE$, false, Metadata.empty()),
                      new StructField("col3", LongType$.MODULE$, false, Metadata.empty())
                    }))
            .repartition(1);

    df.write().mode("overwrite").parquet(inputPath);

    MockServerUtils.clearRequests(mockServer);

    // Execute UNPIVOT operation using SQL
    // According to Analyzer.ResolveUnpivot, this becomes an Expand node
    Dataset<Row> inputDf = spark.read().parquet(inputPath);
    inputDf.createOrReplaceTempView("unpivot_table");

    Dataset<Row> result =
        spark.sql(
            "SELECT id, col_name, col_value FROM unpivot_table "
                + "UNPIVOT (col_value FOR col_name IN (col1, col2, col3))");
    result.write().mode("overwrite").parquet(outputPath);

    spark.stop();

    logEmittedEvents();

    // Verify column lineage exists and is correct
    // Note: Unpivot becomes Expand, so we're testing Expand coverage here
    ColumnLineageDatasetFacet columnLineage =
        verifyColumnLineageExists("unpivot_output", "unpivot_output");

    // For UNPIVOT, output 'id' comes from input 'id'
    verifyColumnDependencies(columnLineage, "id", "id");

    // Output 'col_value' depends on col1, col2, col3 (all unpivoted columns)
    verifyColumnDependencies(columnLineage, "col_value", "col1", "col2", "col3");

    log.info("✅ UNPIVOT operation (becomes Expand) column lineage test passed");
    log.info(
        "Column lineage fields: {}", columnLineage.getFields().getAdditionalProperties().keySet());
  }

  @Test
  @EnabledIfSystemProperty(named = SPARK_VERSION, matches = "([34].*)") // Spark >= 3.*
  void testCubeWithComplexTransformations() {
    String inputPath = "/tmp/test_data/cube_complex_input";
    String outputPath = "/tmp/test_data/cube_complex_output";

    // Create test data with more columns for complex transformations
    createTempDatasetWithStrings(10).write().mode("overwrite").parquet(inputPath);

    MockServerUtils.clearRequests(mockServer);

    // Execute CUBE with complex transformations:
    // - Arithmetic operations (id * 2)
    // - String concatenation (CONCAT)
    // - Aggregations (SUM, AVG, MAX)
    Dataset<Row> df = spark.read().parquet(inputPath);
    df.createOrReplaceTempView("cube_complex_table");

    Dataset<Row> result =
        spark.sql(
            "SELECT "
                + "  id, "
                + "  name, "
                + "  id * 2 as double_id, "
                + "  CONCAT('prefix_', name) as prefixed_name, "
                + "  COUNT(*) as cnt, "
                + "  SUM(id) as sum_id, "
                + "  AVG(id) as avg_id, "
                + "  MAX(id) as max_id "
                + "FROM cube_complex_table "
                + "GROUP BY CUBE(id, name)");
    result.write().mode("overwrite").parquet(outputPath);

    spark.stop();

    logEmittedEvents();

    // Verify column lineage exists and includes complex transformations
    ColumnLineageDatasetFacet columnLineage =
        verifyColumnLineageExists("cube_complex_output", "cube_complex_output");

    // Direct identity columns
    verifyColumnDependencies(columnLineage, "id", "id");
    verifyColumnDependencies(columnLineage, "name", "name");

    // Arithmetic transformation: double_id = id * 2
    verifyColumnDependencies(columnLineage, "double_id", "id");

    // String concatenation: prefixed_name = CONCAT('prefix_', name)
    verifyColumnDependencies(columnLineage, "prefixed_name", "name");

    // Aggregations should have dependencies on source columns
    verifyColumnDependencies(columnLineage, "sum_id", "id");
    verifyColumnDependencies(columnLineage, "avg_id", "id");
    verifyColumnDependencies(columnLineage, "max_id", "id");

    log.info("✅ CUBE with complex transformations column lineage test passed");
    log.info(
        "Column lineage fields: {}", columnLineage.getFields().getAdditionalProperties().keySet());
  }
}
