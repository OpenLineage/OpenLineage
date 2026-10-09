/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent;

import static org.assertj.core.api.Assertions.assertThat;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.OutputDataset;
import io.openlineage.client.OpenLineage.RunEvent;
import io.openlineage.spark.agent.util.DerbyUtils;
import java.nio.file.Path;
import java.util.List;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.io.TempDir;
import org.mockserver.integration.ClientAndServer;

/**
 * Verifies that lineage (both dataset-level inputs and column-level lineage) is preserved across a
 * Spark {@code checkpoint()}/{@code localCheckpoint()} boundary, instead of being lost once the
 * checkpointed data is read back downstream as a {@link org.apache.spark.sql.execution.LogicalRDD}.
 *
 * @see io.openlineage.spark.api.CheckpointContext
 */
@Slf4j
@EnabledIfSystemProperty(named = "spark.version", matches = "([34].*)")
class CheckpointLineageTest {

  @SuppressWarnings("PMD")
  private static final String LOCAL_IP = "127.0.0.1";

  private static final int MOCK_SERVER_PORT = 1091;

  private static ClientAndServer mockServer;
  private SparkSession spark;

  @BeforeAll
  @SneakyThrows
  static void beforeAll() {
    DerbyUtils.loadSystemProperty(CheckpointLineageTest.class.getName());
    Spark4CompatUtils.cleanupAnyExistingSession();
    mockServer = MockServerUtils.createAndConfigureMockServer(MOCK_SERVER_PORT);
  }

  @AfterAll
  @SneakyThrows
  static void afterAll() {
    DerbyUtils.clearDerbyProperty();
    Spark4CompatUtils.cleanupAnyExistingSession();
    MockServerUtils.stopMockServer(mockServer);
  }

  @BeforeEach
  @SneakyThrows
  void beforeEach(@TempDir Path tempDir) {
    MockServerUtils.clearRequests(mockServer);
    spark =
        Spark4CompatUtils.builderWithHiveSupport()
            .master("local[*]")
            .appName("CheckpointLineageTest")
            .config("spark.driver.host", LOCAL_IP)
            .config("spark.driver.bindAddress", LOCAL_IP)
            .config("spark.ui.enabled", false)
            .config("spark.openlineage.transport.type", "http")
            .config(
                "spark.openlineage.transport.url",
                "http://localhost:" + mockServer.getPort() + "/api/v1/namespaces/checkpoint-test")
            .config("spark.openlineage.checkpointLineage.enabled", true)
            .config("spark.extraListeners", OpenLineageSparkListener.class.getName())
            .getOrCreate();
    spark.sparkContext().setCheckpointDir(tempDir.resolve("spark-checkpoints").toString());
    spark.sql("DROP TABLE IF EXISTS checkpoint_t1");
    spark.sql("DROP TABLE IF EXISTS checkpoint_t3");
  }

  @AfterEach
  @SneakyThrows
  void afterEach() {
    spark.sql("DROP TABLE IF EXISTS checkpoint_t1");
    spark.sql("DROP TABLE IF EXISTS checkpoint_t3");
  }

  @Test
  void testReliableCheckpointPreservesLineage() {
    runCheckpointPipeline(df -> df.checkpoint(true));
  }

  @Test
  void testLocalCheckpointPreservesLineage() {
    runCheckpointPipeline(df -> df.localCheckpoint(true));
  }

  private void runCheckpointPipeline(java.util.function.UnaryOperator<Dataset<Row>> checkpointFn) {
    RunEvent complete = runCheckpointPipelineAndGetCompleteEvent(checkpointFn);

    assertThat(complete.getInputs())
        .as(
            "checkpoint should preserve the real upstream table as an input, instead of a "
                + "temporary checkpoint path (or no input at all)")
        .anyMatch(ds -> ds.getName().contains("checkpoint_t1"));

    OutputDataset output =
        complete.getOutputs().stream()
            .filter(ds -> ds.getName().contains("checkpoint_t3"))
            .findFirst()
            .orElseThrow(() -> new AssertionError("No output dataset for checkpoint_t3"));

    OpenLineage.ColumnLineageDatasetFacet columnLineage = output.getFacets().getColumnLineage();
    assertThat(columnLineage).as("columnLineage facet should be present").isNotNull();
    assertThat(columnLineage.getFields().getAdditionalProperties().get("a")).isNotNull();
    assertThat(columnLineage.getFields().getAdditionalProperties().get("a").getInputFields())
        .as("column 'a' of checkpoint_t3 should depend on checkpoint_t1.a")
        .anyMatch(f -> f.getName().contains("checkpoint_t1") && "a".equals(f.getField()));
  }

  private RunEvent runCheckpointPipelineAndGetCompleteEvent(
      java.util.function.UnaryOperator<Dataset<Row>> checkpointFn) {
    spark.sql("CREATE TABLE checkpoint_t1 (a string, b string)");
    spark.sql("INSERT INTO checkpoint_t1 VALUES ('x', 'y')");

    Dataset<Row> checkpointed = checkpointFn.apply(spark.sql("SELECT * FROM checkpoint_t1"));
    checkpointed.createOrReplaceTempView("checkpoint_temp");
    spark.sql("CREATE TABLE checkpoint_t3 AS SELECT * FROM checkpoint_temp");

    List<RunEvent> events =
        MockServerUtils.getEventsEmittedWithJobName(mockServer, "checkpoint_t3");
    return events.stream()
        .filter(e -> e.getEventType() == RunEvent.EventType.COMPLETE)
        .findFirst()
        .orElseThrow(() -> new AssertionError("No COMPLETE event found for checkpoint_t3"));
  }

  /**
   * The feature is opt-in (see {@link
   * io.openlineage.spark.api.SparkOpenLineageConfig.CheckpointConfig}) because the lineage captured
   * for every checkpoint is kept in memory until the checkpointed RDD is read downstream (or
   * evicted), which can cause OOM exceptions for applications checkpointing large/many plans.
   * Verifies that, without explicitly enabling it, checkpointed data is not bridged and the
   * upstream table lineage is lost across the checkpoint boundary, same as before this feature
   * existed.
   */
  @Test
  void testCheckpointLineageDisabledByDefault() throws Exception {
    spark.stop();
    Spark4CompatUtils.cleanupAnyExistingSession();
    spark =
        Spark4CompatUtils.builderWithHiveSupport()
            .master("local[*]")
            .appName("CheckpointLineageDisabledTest")
            .config("spark.driver.host", LOCAL_IP)
            .config("spark.driver.bindAddress", LOCAL_IP)
            .config("spark.ui.enabled", false)
            .config("spark.openlineage.transport.type", "http")
            .config(
                "spark.openlineage.transport.url",
                "http://localhost:" + mockServer.getPort() + "/api/v1/namespaces/checkpoint-test")
            .config("spark.extraListeners", OpenLineageSparkListener.class.getName())
            .getOrCreate();
    spark
        .sparkContext()
        .setCheckpointDir(java.nio.file.Files.createTempDirectory("spark-checkpoints").toString());

    RunEvent complete = runCheckpointPipelineAndGetCompleteEvent(df -> df.checkpoint(true));

    assertThat(complete.getInputs())
        .as("without the feature enabled, the upstream table lineage should not be bridged")
        .noneMatch(ds -> ds.getName().contains("checkpoint_t1"));
  }

  /**
   * Dataset-level dependencies (e.g. a column only referenced in a {@code WHERE} clause, dropped
   * from the output afterwards) aren't tied to any specific output field, so they can't be bridged
   * by matching output attribute names like regular column lineage is. This verifies they're still
   * preserved via {@link io.openlineage.spark.api.Checkpoint#getDatasetDependencyFields()}.
   */
  @Test
  void testCheckpointPreservesDatasetLevelDependencies() {
    spark.sql("CREATE TABLE checkpoint_t1 (a string, b string)");
    spark.sql("INSERT INTO checkpoint_t1 VALUES ('x', 'y')");

    Dataset<Row> checkpointed =
        spark.sql("SELECT b FROM checkpoint_t1 WHERE a = 'x'").checkpoint(true);
    checkpointed.createOrReplaceTempView("checkpoint_temp");
    spark.sql("CREATE TABLE checkpoint_t3 AS SELECT * FROM checkpoint_temp");

    List<RunEvent> events =
        MockServerUtils.getEventsEmittedWithJobName(mockServer, "checkpoint_t3");
    RunEvent complete =
        events.stream()
            .filter(e -> e.getEventType() == RunEvent.EventType.COMPLETE)
            .findFirst()
            .orElseThrow(() -> new AssertionError("No COMPLETE event found for checkpoint_t3"));

    OutputDataset output =
        complete.getOutputs().stream()
            .filter(ds -> ds.getName().contains("checkpoint_t3"))
            .findFirst()
            .orElseThrow(() -> new AssertionError("No output dataset for checkpoint_t3"));

    OpenLineage.ColumnLineageDatasetFacet columnLineage = output.getFacets().getColumnLineage();
    assertThat(columnLineage).as("columnLineage facet should be present").isNotNull();
    assertThat(columnLineage.getDataset())
        .as(
            "dataset-level dependency on checkpoint_t1.a (used only in the checkpointed query's "
                + "WHERE clause, not part of its output) should be preserved")
        .anyMatch(f -> f.getName().contains("checkpoint_t1") && "a".equals(f.getField()));
  }
}
