/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.hive.integration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockserver.model.HttpRequest.request;

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
    assertThat(mockServerClient.retrieveRecordedRequests(request().withPath("/api/v1/lineage")))
        .isEmpty();
  }

  @Test
  public void testFailure() {
    createManagedHiveTable("unemployees", "id int, name string, team int");
    createManagedHiveTable("failure_table", "id int, name string, team int");
    runHiveQuery("INSERT INTO unemployees VALUES(1, 'hello', 1)");
    Assertions.assertThatThrownBy(
            () -> {
              runHiveQuery(
                  String.join(
                      "\n",
                      "INSERT INTO failure_table",
                      "SELECT",
                      "    id,",
                      "    name,",
                      "    ASSERT_TRUE(team != 1) as team", // This will fail for team=1
                      "FROM unemployees"));
            })
        .hasMessageContainingAll("Error while processing statement: FAILED");
    // Check that lineage was still produced
    //        List<OpenLineage.RunEvent> emitted =
    // MockServerTestUtils.getEventsEmitted(mockServerClient);
    //        assertThat(emitted).size().isEqualTo(1);
    //
    // assertThat(emitted.get(0).getEventType()).isEqualTo(OpenLineage.RunEvent.EventType.FAIL);
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
    verifyEvents("simpleCtasComplete.json");
  }
}
