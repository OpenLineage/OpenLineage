/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.hive.integration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockserver.model.HttpRequest.request;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.RunEvent;
import io.openlineage.hive.testutils.MockServerTestUtils;
import java.util.List;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("integration-test")
public class TableLevelLineageTests extends ContainerHiveTestBase {

  public static final String HIVE_TEST_TABLE_DDL = "number INT, text STRING";

  @Test
  public void testSelectAll() {
    createManagedHiveTable("test_table1", HIVE_TEST_TABLE_DDL);
    runHiveQuery("SELECT * FROM " + "test_table1");
    assertThat(mockServerClient.retrieveRecordedRequests(request().withPath("/api/v1/lineage")))
        .isEmpty();
  }

  /** Check that a `INSERT VALUES(...)` query generates no events. */
  @Test
  public void testInsertValues() {
    createManagedHiveTable("test_table2", HIVE_TEST_TABLE_DDL);
    runHiveQuery(String.format("INSERT INTO %s VALUES (99, 'hello')", "test_table2"));
    assertThat(mockServerClient.retrieveRecordedRequests(request().withPath("/api/v1/lineage")))
        .isEmpty();
  }

  @Test
  public void testAddColumn() {
    createManagedHiveTable("test_table3", HIVE_TEST_TABLE_DDL);
    runHiveQuery(String.format("ALTER TABLE %s ADD COLUMNS (fl FLOAT)", "test_table3"));
    assertLifecycle(OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange.ALTER);
  }

  @Test
  public void testCreateTable() {
    runHiveQuery("CREATE TABLE ddl_created (id INT)");
    assertLifecycle(OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange.CREATE);
  }

  @Test
  public void testDropTable() {
    createManagedHiveTable("ddl_dropped", "id INT");
    runHiveQuery("DROP TABLE ddl_dropped");
    assertLifecycle(OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange.DROP);
  }

  @Test
  public void testTruncateTable() {
    createManagedHiveTable("ddl_truncated", "id INT");
    runHiveQuery("TRUNCATE TABLE ddl_truncated");
    assertLifecycle(OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange.TRUNCATE);
  }

  @Test
  public void testRenameTable() {
    createManagedHiveTable("ddl_old", "id INT");
    runHiveQuery("ALTER TABLE ddl_old RENAME TO ddl_new");
    assertLifecycle(OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange.RENAME);
    OpenLineage.OutputDataset output = getEventsEmitted().get(1).getOutputs().get(0);
    assertThat(output.getName()).isEqualTo("test.ddl_new");
    assertThat(output.getFacets().getLifecycleStateChange().getPreviousIdentifier().getName())
        .isEqualTo("test.ddl_old");
  }

  private void assertLifecycle(
      OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange expected) {
    List<RunEvent> emitted = MockServerTestUtils.getEventsEmitted(mockServerClient);
    assertThat(emitted).hasSize(2);
    assertThat(emitted.get(0).getEventType()).isEqualTo(RunEvent.EventType.START);
    assertThat(emitted.get(1).getEventType()).isEqualTo(RunEvent.EventType.COMPLETE);
    assertThat(emitted.get(1).getInputs()).isEmpty();
    assertThat(emitted.get(1).getOutputs())
        .singleElement()
        .satisfies(
            output ->
                assertThat(output.getFacets().getLifecycleStateChange().getLifecycleStateChange())
                    .isEqualTo(expected));
  }

  @Test
  public void testFailure() {
    createManagedHiveTable("unemployees", "id int, name string, team int");
    createManagedHiveTable("failure_table", "id int, name string, team int");
    runHiveQuery("INSERT INTO unemployees VALUES(1, 'hello', 1)");
    Assertions.assertThatThrownBy(
            () ->
                runHiveQuery(
                    String.join(
                        "\n",
                        "INSERT INTO failure_table",
                        "SELECT",
                        "    id,",
                        "    name,",
                        "    ASSERT_TRUE(team != 1) as team", // This will fail for team=1
                        "FROM unemployees")))
        .hasMessageContainingAll("Error while processing statement: FAILED");
    // Check that lineage was still produced
    //        List<OpenLineage.RunEvent> emitted =
    // MockServerTestUtils.getEventsEmitted(mockServerClient);
    //        assertThat(emitted).size().isEqualTo(1);
    //
    // assertThat(emitted.get(0).getEventType()).isEqualTo(OpenLineage.RunEvent.EventType.FAIL);
  }

  @Test
  public void testRunEventsHaveSameRunId() {
    createManagedHiveTable("employees", "id int, name string, team int");
    runHiveQuery("create table result_t as select * from employees");

    List<RunEvent> emitted = MockServerTestUtils.getEventsEmitted(mockServerClient);
    assertThat(emitted).hasSize(2);
    assertThat(emitted.get(0).getEventType()).isEqualTo(RunEvent.EventType.START);
    assertThat(emitted.get(1).getEventType()).isEqualTo(RunEvent.EventType.COMPLETE);
    assertThat(emitted.get(0).getRun().getRunId()).isEqualTo(emitted.get(1).getRun().getRunId());
    assertThat(
            emitted
                .get(1)
                .getOutputs()
                .get(0)
                .getFacets()
                .getLifecycleStateChange()
                .getLifecycleStateChange())
        .isEqualTo(OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange.CREATE);
  }

  @Test
  public void testSimpleCTAS() {
    createManagedHiveTable("employees", "id int, name string, team int");
    createManagedHiveTable("managers", "id int, name string, team int");
    createManagedHiveTable("teams", "id int, type int, building string");
    runHiveQuery(
        String.join(
            "\n",
            "create table result_t as",
            "select",
            "teams.type,",
            "teams.building,",
            "managers.name as manager,",
            "employees.name as employee",
            "from teams, managers, employees",
            "where teams.id = managers.team and teams.id = employees.team"));
    verifyEvents("simpleCtasStart.json", "simpleCtasComplete.json");
  }

  @Test
  public void testSimpleExport() {
    createManagedHiveTable("export_source", "id int, name string, value double");
    runHiveQuery("INSERT INTO export_source VALUES (1, 'test1', 10.5), (2, 'test2', 20.7)");
    runHiveQuery("EXPORT TABLE export_source TO '/tmp/export_test'");
    verifyEvents("simpleExportStart.json", "simpleExportComplete.json");
  }

  @Test
  public void testPartitionedExport() {
    createPartitionedHiveTable(
        "partitioned_export_source", "id int, name string, value double", "year int");
    runHiveQuery(
        "INSERT INTO partitioned_export_source PARTITION(year=2023) VALUES (1, 'test1', 10.5), (2, 'test2', 20.7)");
    runHiveQuery(
        "INSERT INTO partitioned_export_source PARTITION(year=2024) VALUES (3, 'test3', 30.9), (4, 'test4', 40.1)");
    runHiveQuery(
        "EXPORT TABLE partitioned_export_source PARTITION(year=2023) TO '/tmp/export_partition_test'");
    verifyEvents("partitionedExportStart.json", "partitionedExportComplete.json");
  }

  @Test
  public void testSimpleLoad() {
    createManagedHiveTable("load_target", "id int, name string, value double");
    // First create a data file to load from
    runHiveQuery("INSERT INTO load_target VALUES (1, 'test1', 10.5), (2, 'test2', 20.7)");
    runHiveQuery("INSERT OVERWRITE LOCAL DIRECTORY '/tmp/load_data' SELECT * FROM load_target");
    // Clear the target table
    runHiveQuery("TRUNCATE TABLE load_target");
    // Now perform the LOAD operation
    runHiveQuery("LOAD DATA LOCAL INPATH '/tmp/load_data' INTO TABLE load_target");
    verifyEvents("loadStart.json", "loadComplete.json");
  }

  @Test
  public void testLoadToPartition() {
    createPartitionedHiveTable(
        "partitioned_load_target", "id int, name string, value double", "year int");
    // Create source data file
    runHiveQuery(
        "INSERT OVERWRITE LOCAL DIRECTORY '/tmp/load_partition_data' SELECT 1, 'test1', 10.5 UNION ALL SELECT 2, 'test2', 20.7");
    // Load data into specific partition
    runHiveQuery(
        "LOAD DATA LOCAL INPATH '/tmp/load_partition_data' INTO TABLE partitioned_load_target PARTITION(year=2023)");
    verifyEvents("loadToPartitionStart.json", "loadToPartitionComplete.json");
  }

  @Test
  public void testSimpleImport() {
    // First create and export a table to have data to import
    createManagedHiveTable("export_for_import", "id int, name string, value double");
    runHiveQuery("INSERT INTO export_for_import VALUES (1, 'test1', 10.5), (2, 'test2', 20.7)");
    runHiveQuery("EXPORT TABLE export_for_import TO '/tmp/export_for_import';");

    // Now import the exported data into a new table
    runHiveQuery("IMPORT TABLE imported_table FROM '/tmp/export_for_import'");
    verifyEvents("importStart.json", "importComplete.json");
  }
}
