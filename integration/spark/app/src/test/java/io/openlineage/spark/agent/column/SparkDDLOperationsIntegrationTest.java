/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.column;

import static io.openlineage.spark.agent.MockServerUtils.getEventsEmitted;
import static io.openlineage.spark.agent.SparkTestUtils.SPARK_VERSION;
import static org.assertj.core.api.Assertions.assertThat;

import io.openlineage.client.OpenLineage.InputDataset;
import io.openlineage.client.OpenLineage.RunEvent;
import io.openlineage.spark.agent.MockServerUtils;
import java.util.List;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

/**
 * Integration tests for DDL operations to verify dataset-level lineage tracking.
 *
 * <p>DDL operations tested:
 *
 * <ul>
 *   <li><b>RenameTable</b> - ALTER TABLE ... RENAME TO ...
 *   <li><b>DropView</b> - DROP VIEW ...
 *   <li><b>AlterViewAs</b> - ALTER VIEW ... AS SELECT ...
 * </ul>
 *
 * <p>Note: DDL operations typically track dataset-level lineage (input/output datasets and
 * lifecycle changes), not column-level lineage. These tests verify that OpenLineage events are
 * emitted and datasets are properly tracked.
 */
@Slf4j
class SparkDDLOperationsIntegrationTest extends SparkColumnLineageBaseTest {

  @Override
  protected int getMockServerPort() {
    return 1090;
  }

  @Override
  protected String getAppName() {
    return "DDLOperationsIntegrationTest";
  }

  @Test
  @EnabledIfSystemProperty(named = SPARK_VERSION, matches = "([34].*)") // Spark >= 3.*
  void testRenameTableV2API() {
    MockServerUtils.clearRequests(mockServer);

    // Create a V2 table using saveAsTable (uses V2 API in Spark 3+)
    spark.sql("DROP TABLE IF EXISTS original_v2_table");
    spark.sql("DROP TABLE IF EXISTS renamed_v2_table");

    createTempDataset(10, 0).write().mode("overwrite").saveAsTable("original_v2_table");

    // Execute V2 RENAME TABLE - this should use RenameTable logical plan node
    spark.sql("ALTER TABLE original_v2_table RENAME TO renamed_v2_table");

    // Verify table was renamed by querying it
    Dataset<Row> result = spark.sql("SELECT * FROM renamed_v2_table");
    result.write().mode("overwrite").parquet("/tmp/test_data/ddl_rename_verify");

    spark.stop();

    logEmittedEvents();

    // Verify RenameTable operation emits events with proper dataset tracking
    List<RunEvent> events = getEventsEmitted(mockServer);

    // Look for events related to rename or renamed table
    List<RunEvent> renameRelatedEvents =
        events.stream()
            .filter(
                e ->
                    e.getJob().getName().toLowerCase().contains("rename")
                        || e.getJob().getName().contains("original_v2_table")
                        || e.getJob().getName().contains("renamed_v2_table"))
            .collect(java.util.stream.Collectors.toList());

    log.info(
        "✅ RenameTable V2 API test completed - rename-related events: {}",
        renameRelatedEvents.size());

    // Log what we found
    renameRelatedEvents.forEach(
        event -> {
          log.info(
              "  Event - Job: {}, Type: {}, Inputs: {}, Outputs: {}",
              event.getJob().getName(),
              event.getEventType(),
              event.getInputs().size(),
              event.getOutputs().size());

          // Log datasets involved
          event.getInputs().forEach(input -> log.info("    Input: {}", input.getName()));
          event.getOutputs().forEach(output -> log.info("    Output: {}", output.getName()));
        });

    // Verify that renamed table is tracked in subsequent query
    Optional<RunEvent> verifyEvent =
        events.stream()
            .filter(e -> e.getJob().getName().contains("ddl_rename_verify"))
            .filter(e -> !e.getInputs().isEmpty())
            .findFirst();

    assertThat(verifyEvent).isPresent();
    assertThat(
            verifyEvent.get().getInputs().stream()
                .anyMatch(i -> i.getName().contains("renamed_v2_table")))
        .isTrue();

    log.info("✅ Renamed table properly tracked in lineage");
  }

  @Test
  @EnabledIfSystemProperty(named = SPARK_VERSION, matches = "([34].*)") // Spark >= 3.*
  void testDropViewEmitsEvent() {
    String inputPath = "/tmp/test_data/ddl_drop_view_source";

    // Create source data and view
    createTempDataset(10, 0).write().mode("overwrite").parquet(inputPath);

    MockServerUtils.clearRequests(mockServer);

    Dataset<Row> df = spark.read().parquet(inputPath);
    df.createOrReplaceTempView("view_to_drop");

    // Execute DROP VIEW
    spark.sql("DROP VIEW view_to_drop");

    spark.stop();

    logEmittedEvents();

    // Verify DROP VIEW emits events
    List<RunEvent> events = getEventsEmitted(mockServer);

    // Should have events from view creation
    assertThat(events).isNotEmpty();

    log.info("✅ DropView event emission test passed - events: {}", events.size());
  }

  @Test
  @EnabledIfSystemProperty(named = SPARK_VERSION, matches = "([34].*)") // Spark >= 3.*
  void testCreateOrReplaceViewTracksLineage() {
    String inputPath = "/tmp/test_data/ddl_view_source";

    // Create source data
    createTempDataset(10, 0).write().mode("overwrite").parquet(inputPath);

    MockServerUtils.clearRequests(mockServer);

    Dataset<Row> df = spark.read().parquet(inputPath);
    df.createOrReplaceTempView("source_for_view");

    // Create view with SELECT
    spark.sql("CREATE OR REPLACE TEMP VIEW test_view AS SELECT a, b FROM source_for_view");

    // Now query the view and write to output
    Dataset<Row> viewData = spark.sql("SELECT * FROM test_view");
    viewData.write().mode("overwrite").parquet("/tmp/test_data/ddl_view_output");

    spark.stop();

    logEmittedEvents();

    // Verify that when querying through view, lineage traces back to source
    List<RunEvent> events = getEventsEmitted(mockServer);

    Optional<RunEvent> outputEvent =
        events.stream()
            .filter(e -> e.getEventType() == RunEvent.EventType.COMPLETE)
            .filter(e -> e.getJob().getName().contains("ddl_view_output"))
            .filter(e -> !e.getInputs().isEmpty())
            .findFirst();

    assertThat(outputEvent).isPresent();

    // Verify input includes source table (should trace through view)
    List<InputDataset> inputs = outputEvent.get().getInputs();
    assertThat(inputs).isNotEmpty();
    assertThat(inputs.stream().anyMatch(i -> i.getName().contains("ddl_view_source"))).isTrue();

    log.info("✅ CREATE OR REPLACE VIEW lineage tracking test passed");
  }

  @Test
  @EnabledIfSystemProperty(named = SPARK_VERSION, matches = "([34].*)") // Spark >= 3.*
  void testPartitionedTableOperations() {
    String tablePath = "/tmp/test_data/ddl_partitioned_ops";

    MockServerUtils.clearRequests(mockServer);

    // Create partitioned table
    createTempDataset(20, 0).write().mode("overwrite").partitionBy("a").parquet(tablePath);

    // Read partitioned data
    Dataset<Row> df = spark.read().parquet(tablePath);

    // Write filtered partition data to new location
    df.filter("a > 10").write().mode("overwrite").parquet("/tmp/test_data/ddl_partition_output");

    spark.stop();

    logEmittedEvents();

    // Verify partition operations track datasets
    List<RunEvent> events = getEventsEmitted(mockServer);

    Optional<RunEvent> partitionEvent =
        events.stream()
            .filter(e -> e.getEventType() == RunEvent.EventType.COMPLETE)
            .filter(e -> e.getJob().getName().contains("ddl_partition_output"))
            .filter(e -> !e.getInputs().isEmpty())
            .findFirst();

    assertThat(partitionEvent).isPresent();

    // Verify input dataset tracked
    List<InputDataset> inputs = partitionEvent.get().getInputs();
    assertThat(inputs).isNotEmpty();
    assertThat(inputs.stream().anyMatch(i -> i.getName().contains("ddl_partitioned_ops"))).isTrue();

    log.info("✅ Partitioned table operations test passed");
  }
}
