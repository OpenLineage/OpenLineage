/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent;

import static org.assertj.core.api.Assertions.assertThat;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.OutputDataset;
import io.openlineage.client.OpenLineage.RunEvent;
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
 * <p>Tables are created with the in-memory catalog and a per-test warehouse directory, so no
 * embedded Hive metastore (Derby) is started. This lets each test build its own Spark session
 * without the metastore being booted twice in the same JVM.
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

  @TempDir Path tempDir;

  @BeforeAll
  @SneakyThrows
  static void beforeAll() {
    Spark4CompatUtils.cleanupAnyExistingSession();
    mockServer = MockServerUtils.createAndConfigureMockServer(MOCK_SERVER_PORT);
  }

  @AfterAll
  @SneakyThrows
  static void afterAll() {
    Spark4CompatUtils.cleanupAnyExistingSession();
    MockServerUtils.stopMockServer(mockServer);
  }

  @BeforeEach
  @SneakyThrows
  void beforeEach() {
    MockServerUtils.clearRequests(mockServer);
    spark = createSession(true);
  }

  @AfterEach
  @SneakyThrows
  void afterEach() {
    spark.stop();
    Spark4CompatUtils.cleanupAnyExistingSession();
  }

  private SparkSession createSession(boolean checkpointLineageEnabled) {
    SparkSession.Builder builder =
        SparkSession.builder()
            .master("local[*]")
            .appName("CheckpointLineageTest")
            .config("spark.driver.host", LOCAL_IP)
            .config("spark.driver.bindAddress", LOCAL_IP)
            .config("spark.ui.enabled", false)
            .config("spark.sql.catalogImplementation", "in-memory")
            .config("spark.sql.warehouse.dir", tempDir.resolve("warehouse").toString())
            .config("spark.openlineage.transport.type", "http")
            .config(
                "spark.openlineage.transport.url",
                "http://localhost:" + mockServer.getPort() + "/api/v1/namespaces/checkpoint-test")
            .config("spark.extraListeners", OpenLineageSparkListener.class.getName());
    if (checkpointLineageEnabled) {
      builder.config("spark.openlineage.checkpointLineage.enabled", true);
    }
    SparkSession session = builder.getOrCreate();
    session.sparkContext().setCheckpointDir(tempDir.resolve("spark-checkpoints").toString());
    return session;
  }

  @Test
  @SuppressWarnings("PMD.JUnitTestsShouldIncludeAssert")
  void testNonLocalCheckpointPreservesLineage() {
    runCheckpointPipeline(df -> df.checkpoint(true));
  }

  @Test
  @SuppressWarnings("PMD.JUnitTestsShouldIncludeAssert")
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

    OpenLineage.ColumnLineageDatasetFacet columnLineage = columnLineageOfOutput(complete);
    assertThat(columnLineage.getFields().getAdditionalProperties().get("a")).isNotNull();
    assertThat(columnLineage.getFields().getAdditionalProperties().get("a").getInputFields())
        .as("column 'a' of checkpoint_t3 should depend on checkpoint_t1.a")
        .anyMatch(f -> f.getName().contains("checkpoint_t1") && "a".equals(f.getField()));

    assertThat(columnLineage.getFields().getAdditionalProperties().get("b")).isNotNull();
    assertThat(columnLineage.getFields().getAdditionalProperties().get("b").getInputFields())
        .as(
            "column 'b' of checkpoint_t3 should depend on checkpoint_t1.b through the upper() "
                + "transformation applied before the checkpoint, not an identity")
        .anyMatch(
            f ->
                f.getName().contains("checkpoint_t1")
                    && "b".equals(f.getField())
                    && f.getTransformations().stream()
                        .anyMatch(
                            t ->
                                "DIRECT".equals(t.getType())
                                    && "TRANSFORMATION".equals(t.getSubtype())));
  }

  private RunEvent runCheckpointPipelineAndGetCompleteEvent(
      java.util.function.UnaryOperator<Dataset<Row>> checkpointFn) {
    return runCheckpointPipelineAndGetCompleteEvent(
        "SELECT a, upper(b) AS b FROM checkpoint_t1 WHERE a = 'x'", checkpointFn);
  }

  private RunEvent runCheckpointPipelineAndGetCompleteEvent(
      String checkpointedQuery, java.util.function.UnaryOperator<Dataset<Row>> checkpointFn) {
    return runCheckpointPipelineAndGetCompleteEvent(
        checkpointedQuery, "SELECT * FROM checkpoint_temp", checkpointFn);
  }

  private RunEvent runCheckpointPipelineAndGetCompleteEvent(
      String checkpointedQuery,
      String downstreamQuery,
      java.util.function.UnaryOperator<Dataset<Row>> checkpointFn) {
    spark.sql("CREATE TABLE checkpoint_t1 (a string, b string) USING parquet");
    spark.sql("INSERT INTO checkpoint_t1 VALUES ('x', 'y')");

    Dataset<Row> checkpointed = checkpointFn.apply(spark.sql(checkpointedQuery));
    checkpointed.createOrReplaceTempView("checkpoint_temp");
    spark.sql("CREATE TABLE checkpoint_t3 USING parquet AS " + downstreamQuery);

    List<RunEvent> events =
        MockServerUtils.getEventsEmittedWithJobName(mockServer, "checkpoint_t3");
    return events.stream()
        .filter(e -> e.getEventType() == RunEvent.EventType.COMPLETE)
        .findFirst()
        .orElseThrow(() -> new AssertionError("No COMPLETE event found for checkpoint_t3"));
  }

  private static OpenLineage.ColumnLineageDatasetFacet columnLineageOfOutput(RunEvent complete) {
    OutputDataset output =
        complete.getOutputs().stream()
            .filter(ds -> ds.getName().contains("checkpoint_t3"))
            .findFirst()
            .orElseThrow(() -> new AssertionError("No output dataset for checkpoint_t3"));
    OpenLineage.ColumnLineageDatasetFacet columnLineage = output.getFacets().getColumnLineage();
    assertThat(columnLineage).as("columnLineage facet should be present").isNotNull();
    return columnLineage;
  }

  /**
   * The feature is opt-in (see {@link
   * io.openlineage.spark.api.SparkOpenLineageConfig.CheckpointConfig}) because the lineage captured
   * for every checkpoint is kept in memory until evicted, regardless of whether it has been read,
   * which can cause OOM exceptions for applications checkpointing large plans. Verifies that,
   * without explicitly enabling it, checkpointed data is not bridged and the upstream table lineage
   * is lost across the checkpoint boundary, same as before this feature existed.
   */
  @Test
  void testCheckpointLineageDisabledByDefault() {
    spark.stop();
    Spark4CompatUtils.cleanupAnyExistingSession();
    spark = createSession(false);

    RunEvent complete = runCheckpointPipelineAndGetCompleteEvent(df -> df.checkpoint(true));

    assertThat(complete.getInputs())
        .as("without the feature enabled, the upstream table lineage should not be bridged")
        .noneMatch(ds -> ds.getName().contains("checkpoint_t1"));
  }

  /**
   * Dataset-level dependencies (e.g. a column only referenced in a {@code WHERE} clause, dropped
   * from the output afterwards) aren't tied to any specific output field, so they can't be bridged
   * by matching output attribute names like regular column lineage is. This verifies they're still
   * preserved via {@link
   * io.openlineage.spark.agent.lifecycle.plan.column.CheckpointColumnLineage#getDatasetDependencyInputs()}.
   */
  @Test
  void testCheckpointPreservesDatasetLevelDependencies() {
    RunEvent complete =
        runCheckpointPipelineAndGetCompleteEvent(
            "SELECT b FROM checkpoint_t1 WHERE a = 'x'", df -> df.checkpoint(true));

    assertThat(columnLineageOfOutput(complete).getDataset())
        .as(
            "dataset-level dependency on checkpoint_t1.a (used only in the checkpointed query's "
                + "WHERE clause, not part of its output) should be preserved")
        .anyMatch(f -> f.getName().contains("checkpoint_t1") && "a".equals(f.getField()));
  }

  /**
   * A checkpointed plan without any field-level lineage (e.g. a global count) can still have
   * dataset-level dependencies, which must not be dropped along with the empty field lineage. The
   * downstream query joins back to {@code checkpoint_t1.b} only so that it has field-level lineage
   * of its own - otherwise its column lineage facet would be skipped altogether.
   */
  @Test
  void testCheckpointPreservesDatasetLevelDependenciesWithoutFieldLineage() {
    RunEvent complete =
        runCheckpointPipelineAndGetCompleteEvent(
            "SELECT count(*) AS c FROM checkpoint_t1 WHERE a = 'x'",
            "SELECT c.c, t.b FROM checkpoint_temp c CROSS JOIN checkpoint_t1 t",
            df -> df.checkpoint(true));

    assertThat(columnLineageOfOutput(complete).getDataset())
        .as("dataset-level dependency on checkpoint_t1.a should be preserved")
        .anyMatch(f -> f.getName().contains("checkpoint_t1") && "a".equals(f.getField()));
  }

  /**
   * A checkpointed plan with a shuffle runs as a job with multiple stages, only one of which (the
   * result stage) computes the checkpointed RDD.
   */
  @Test
  void testMultiStageCheckpointPreservesLineage() {
    spark.conf().set("spark.sql.adaptive.enabled", "false");

    RunEvent complete =
        runCheckpointPipelineAndGetCompleteEvent(
            "SELECT a, max(b) AS b FROM checkpoint_t1 GROUP BY a", df -> df.checkpoint(true));

    assertThat(complete.getInputs())
        .as("checkpoint should preserve the real upstream table as an input")
        .anyMatch(ds -> ds.getName().contains("checkpoint_t1"));
    assertThat(columnLineageOfOutput(complete).getFields().getAdditionalProperties().get("b"))
        .isNotNull()
        .satisfies(
            field ->
                assertThat(field.getInputFields())
                    .anyMatch(
                        f -> f.getName().contains("checkpoint_t1") && "b".equals(f.getField())));
  }

  /**
   * A lazy local checkpoint persists the checkpointed RDD right away, so it can be found among the
   * persisted RDDs even though no job computes it within the checkpoint's own execution.
   */
  @Test
  @SuppressWarnings("PMD.JUnitTestsShouldIncludeAssert")
  void testLazyLocalCheckpointPreservesLineage() {
    runCheckpointPipeline(df -> df.localCheckpoint(false));
  }

  /**
   * A lazy checkpoint is only materialized by the first job reading it, so the checkpointed RDD
   * isn't reported as checkpointed yet while that job runs. Its lineage should still be attached to
   * all events of the job, not only to the ones emitted after it completes.
   */
  @Test
  void testLazyLocalCheckpointFirstReadEventsHaveLineage() {
    runCheckpointPipelineAndGetCompleteEvent(df -> df.localCheckpoint(false));

    List<RunEvent> events =
        MockServerUtils.getEventsEmittedWithJobName(mockServer, "checkpoint_t3");
    assertThat(events)
        .filteredOn(e -> e.getEventType() == RunEvent.EventType.START)
        .isNotEmpty()
        .allSatisfy(
            start -> {
              assertThat(start.getInputs()).anyMatch(ds -> ds.getName().contains("checkpoint_t1"));
              assertThat(
                      columnLineageOfOutput(start)
                          .getFields()
                          .getAdditionalProperties()
                          .get("b")
                          .getInputFields())
                  .anyMatch(f -> f.getName().contains("checkpoint_t1") && "b".equals(f.getField()));
            });
  }

  /**
   * Lazy non-local checkpoints aren't materialized within their own execution and the checkpointed
   * RDD can't be reached otherwise, so their lineage can't be captured - they should behave the
   * same as with the feature disabled.
   */
  @Test
  void testLazyCheckpointIsNotBridged() {
    RunEvent complete = runCheckpointPipelineAndGetCompleteEvent(df -> df.checkpoint(false));

    assertThat(complete.getInputs())
        .as(
            "lazy non-local checkpoints are not supported, so upstream table lineage should not be "
                + "bridged")
        .noneMatch(ds -> ds.getName().contains("checkpoint_t1"));
  }
}
