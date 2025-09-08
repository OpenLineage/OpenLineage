/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent;

import static io.openlineage.spark.agent.MockServerUtils.getEventsEmitted;
import static org.assertj.core.api.Assertions.assertThat;

import io.openlineage.client.OpenLineage.InputDataset;
import io.openlineage.client.OpenLineage.OutputDataset;
import io.openlineage.client.OpenLineage.RunEvent;
import java.util.List;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockserver.integration.ClientAndServer;

/**
 * Integration test for CreateDataSourceTableAsSelectCommand to verify that OpenLineage correctly
 * captures input datasets from CTAS operations.
 *
 * <p>Test scenario: 1. Create a source table with test data 2. Execute CREATE TABLE AS SELECT
 * (CTAS) command 3. Verify that lineage events capture the parquet input dataset
 */
@Tag("integration-test")
@Slf4j
class CreateDataSourceTableAsSelectCommandIntegrationTest {

  @SuppressWarnings("PMD")
  private static final String LOCAL_IP = "127.0.0.1";

  private static final int MOCK_SERVER_PORT = 1084;
  private static final String WAREHOUSE_LOCATION = "/tmp/spark-warehouse";
  private static final String METASTORE_DB_LOCATION = "/tmp/metastore_db";
  private static final String TEST_NAMESPACE = "ctas-integration-test";

  private static SparkSession spark;
  private static ClientAndServer mockServer;

  @BeforeAll
  @SneakyThrows
  public static void beforeAll() {
    Spark4CompatUtils.cleanupAnyExistingSession();
    mockServer = MockServerUtils.createAndConfigureMockServer(MOCK_SERVER_PORT);
  }

  @AfterAll
  @SneakyThrows
  public static void afterAll() {
    Spark4CompatUtils.cleanupAnyExistingSession();
    MockServerUtils.stopMockServer(mockServer);
  }

  @BeforeEach
  @SneakyThrows
  public void beforeEach() {
    MockServerUtils.clearRequests(mockServer);
    spark = createTestSparkSession();
  }

  /** Creates a SparkSession configured for CTAS integration testing. */
  private static SparkSession createTestSparkSession() {
    return SparkSession.builder()
        .master("local[*]")
        .appName("CTAS_IntegrationTest")
        .config("spark.driver.host", LOCAL_IP)
        .config("spark.driver.bindAddress", LOCAL_IP)
        .config("spark.ui.enabled", false)
        .config("spark.sql.shuffle.partitions", 1)
        .config("spark.sql.catalogImplementation", "hive")
        .config("spark.sql.warehouse.dir", WAREHOUSE_LOCATION)
        .config(
            "javax.jdo.option.ConnectionURL",
            "jdbc:derby:;databaseName=" + METASTORE_DB_LOCATION + ";create=true")
        .config("spark.openlineage.transport.type", "http")
        .config(
            "spark.openlineage.transport.url",
            "http://localhost:" + mockServer.getPort() + "/api/v1/lineage")
        .config("spark.openlineage.facets.debug.disabled", "false")
        .config("spark.openlineage.namespace", TEST_NAMESPACE)
        .config("spark.openlineage.parentJobName", "ctas-parent-job")
        .config("spark.openlineage.parentRunId", "ctas-bd9c2467-3ed7-4fdc-85c2-41ebf5c73b40")
        .config("spark.openlineage.parentJobNamespace", "ctas-parent-namespace")
        .config("spark.extraListeners", OpenLineageSparkListener.class.getName())
        .enableHiveSupport()
        .getOrCreate();
  }

  /** Creates a test dataset with sample data for CTAS operations. */
  private static Dataset<Row> getTestDataset() {
    StructType schema = new StructType().add("city", DataTypes.StringType, true);

    List<Row> data =
        List.of(
            RowFactory.create("New York"),
            RowFactory.create("London"),
            RowFactory.create("Paris"),
            RowFactory.create("Tokyo"));

    return spark.createDataFrame(data, schema);
  }

  /** Creates the source table used in CTAS operations. */
  private void createSourceTable() {
    log.info("Creating source table: db1.table1");

    // Clean up any existing table location
    cleanupTableLocation("/tmp/spark-warehouse/db1.db/table1");

    Dataset<Row> df = getTestDataset();

    // Create database and table
    spark.sql("CREATE DATABASE IF NOT EXISTS db1");
    df.write().mode("overwrite").saveAsTable("db1.table1");

    log.info("Source table created successfully.");
  }

  /** Executes the CTAS operation and returns the target table name. */
  private void executeCtasOperation() {
    Dataset<Row> sourceDf = spark.read().table("db1.table1");

    if (log.isDebugEnabled()) {
      log.debug("Logical plan before CTAS: {}", sourceDf.queryExecution().logical().toString());
      log.debug("Optimized plan: {}", sourceDf.queryExecution().optimizedPlan().toJSON());
    }

    spark.sql("CREATE DATABASE IF NOT EXISTS db2");
    String outputTable = "db2.table2";

    // Clean up any existing output table location
    cleanupTableLocation("/tmp/spark-warehouse/db2.db/table2");

    sourceDf.write().mode("overwrite").saveAsTable(outputTable);
  }

  /** Cleans up a table location by deleting the directory if it exists. */
  private void cleanupTableLocation(String tablePath) {
    try {
      java.nio.file.Path path = java.nio.file.Paths.get(tablePath);
      if (java.nio.file.Files.exists(path)) {
        java.nio.file.Files.walk(path)
            .sorted(java.util.Comparator.reverseOrder())
            .map(java.nio.file.Path::toFile)
            .forEach(java.io.File::delete);
        log.debug("Cleaned up table location: {}", tablePath);
      }
    } catch (Exception e) {
      log.warn("Failed to clean up table location {}: {}", tablePath, e.getMessage());
    }
  }

  /** Finds the CTAS job completion event from the list of events. */
  private RunEvent findCtasJobEvent(List<RunEvent> events) {
    String expectedJobPattern = "db2_table2";

    return events.stream()
        .filter(event -> event.getEventType() == RunEvent.EventType.COMPLETE)
        .filter(
            event ->
                event
                    .getJob()
                    .getName()
                    .contains("execute_create_data_source_table_as_select_command"))
        .filter(event -> event.getJob().getName().contains(expectedJobPattern))
        .findFirst()
        .orElseThrow(() -> new AssertionError("CTAS job completion event not found"));
  }

  /** Asserts that the CTAS job contains the expected parquet input dataset. */
  private void assertContainsInput(RunEvent ctasEvent) {
    List<InputDataset> inputs = ctasEvent.getInputs();
    String expectedInputPath = "/tmp/spark-warehouse/db1.db/table1";

    boolean foundParquetInput =
        inputs.stream()
            .anyMatch(
                input ->
                    "file".equals(input.getNamespace())
                        && input.getName().contains(expectedInputPath));

    assertThat(foundParquetInput)
        .withFailMessage(
            "Expected CTAS job to have parquet input dataset from %s, but it was not found",
            expectedInputPath)
        .isTrue();

    log.info("✓ Found parquet input dataset: file:{}", expectedInputPath);
  }

  /** Asserts that the CTAS job contains the expected table output dataset. */
  private void assertContainsTableOutput(RunEvent ctasEvent) {
    List<OutputDataset> outputs = ctasEvent.getOutputs();
    String expectedOutputPath = "/tmp/spark-warehouse/db2.db/table2";

    boolean foundTableOutput =
        outputs.stream()
            .anyMatch(
                output ->
                    "file".equals(output.getNamespace())
                        && output.getName().contains(expectedOutputPath));

    assertThat(foundTableOutput)
        .withFailMessage(
            "Expected CTAS job to have table output dataset for %s, but it was not found",
            expectedOutputPath)
        .isTrue();

    log.info("✓ Found table output dataset: file:{}", expectedOutputPath);
  }

  /** Asserts that the CTAS job has exactly one input and one output. */
  private void assertCorrectDatasetCounts(RunEvent ctasEvent) {
    List<InputDataset> inputs = ctasEvent.getInputs();
    List<OutputDataset> outputs = ctasEvent.getOutputs();

    assertThat(inputs)
        .withFailMessage(
            "Expected CTAS job to have exactly 1 input dataset, but found %d", inputs.size())
        .hasSize(1);

    assertThat(outputs)
        .withFailMessage(
            "Expected CTAS job to have exactly 1 output dataset, but found %d", outputs.size())
        .hasSize(1);

    log.info("✓ Correct dataset counts: 1 input, 1 output");
  }

  @Test
  void testCreateDataSourceTableAsSelectCapturesInput() {
    // Setup: Create the source table
    createSourceTable();

    // Execute: Perform CTAS operation
    executeCtasOperation();

    // Verify: Check lineage events
    List<RunEvent> events = getEventsEmitted(mockServer);
    RunEvent ctasEvent = findCtasJobEvent(events);

    log.info("Found CTAS job: {}", ctasEvent.getJob().getName());

    // Assert that the CTAS operation captured the correct lineage
    assertContainsInput(ctasEvent);
    assertContainsTableOutput(ctasEvent);
    assertCorrectDatasetCounts(ctasEvent);

    spark.stop();
  }
}
