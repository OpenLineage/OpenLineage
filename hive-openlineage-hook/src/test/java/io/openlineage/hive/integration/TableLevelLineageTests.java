/*
 * Copyright 2024 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.openlineage.hive.integration;

import static io.openlineage.hive.testutils.OLUtils.*;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

import com.google.cloud.datacatalog.lineage.v1.LineageEvent;
import com.google.cloud.datacatalog.lineage.v1.Link;
import io.openlineage.client.OpenLineage.BaseEvent;
import io.openlineage.client.OpenLineage.RunEvent;
import io.openlineage.hive.TestsBase;
import io.openlineage.hive.testutils.DataplexTestUtils;
import io.openlineage.hive.transport.DummyTransport;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.List;
import org.junit.jupiter.api.Test;

public class TableLevelLineageTests extends TestsBase {

  public static final String TEST_TABLE_NAME = "mytable";
  public static final String ANOTHER_TEST_TABLE_NAME = "anothertable";
  public static final String HIVE_TEST_TABLE_DDL = "number INT, text STRING";
  public static String BIGQUERY_TEST_TABLE_DDL = String.join("\n", "number INT64,", "text STRING");

  /** Check that a SELECT query generates no events. */
  @Test
  public void testSelectAll() throws IOException {
    createManagedHiveTable(TEST_TABLE_NAME, HIVE_TEST_TABLE_DDL);
    runHiveQuery("SELECT * FROM " + TEST_TABLE_NAME);
    assertThat(DummyTransport.getEvents()).isEmpty();
    assertThat(getDataplexProcess()).isNull();
  }

  /** Check that a `INSERT VALUES(...)` query generates no events. */
  @Test
  public void testInsertValues() throws IOException {
    createManagedHiveTable(TEST_TABLE_NAME, HIVE_TEST_TABLE_DDL);
    runHiveQuery(String.format("INSERT INTO %s VALUES (99, 'hello')", TEST_TABLE_NAME));
    assertThat(DummyTransport.getEvents()).isEmpty();
    assertThat(getDataplexProcess()).isNull();
  }

  @Test
  public void testAddColumn() throws IOException {
    createManagedHiveTable(TEST_TABLE_NAME, HIVE_TEST_TABLE_DDL);
    runHiveQuery(String.format("ALTER TABLE %s ADD COLUMNS (fl FLOAT)", TEST_TABLE_NAME));
    assertThat(DummyTransport.getEvents()).isEmpty();
    assertThat(getDataplexProcess()).isNull();
  }

  @Test
  public void testSimpleCTAS() throws IOException {
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
    // Check the OpenLineage event
    List<BaseEvent> olEvents = DummyTransport.getEvents();
    assertThat(olEvents).hasSize(1);
    RunEvent runEvent = (RunEvent) olEvents.get(0);
    assertStandardFormat(runEvent, hive.getHiveConf(), olJobNamespace, olJobName);
    assertThat(runEvent.getInputs()).hasSize(3);
    assertDatasets(
        runEvent.getInputs(),
        new HashSet<>(
            asList(
                createDatasetMap("employees", asList("id:int", "name:string", "team:int")),
                createDatasetMap("managers", asList("id:int", "name:string", "team:int")),
                createDatasetMap("teams", asList("id:int", "type:int", "building:string")))));
    assertThat(runEvent.getOutputs()).hasSize(1);
    assertDatasets(
        runEvent.getOutputs(),
        new HashSet<>(
            asList(
                createDatasetMap(
                    "result_t",
                    asList("type:int", "building:string", "manager:string", "employee:string")))));
    // Check the Dataplex event
    List<LineageEvent> dataplexEvents = getDataplexEvents();
    assertThat(dataplexEvents).hasSize(1);
    DataplexTestUtils.assertDataplexLinks(
        new HashSet<>(
            asList(
                asList("employees", "result_t"),
                asList("managers", "result_t"),
                asList("teams", "result_t"))),
        dataplexEvents.get(0).getLinksList());
  }

  @Test
  public void testInsertIntoBigQuery() throws InterruptedException, IOException {
    createExternalHiveBigQueryTable(TEST_TABLE_NAME, HIVE_TEST_TABLE_DDL, BIGQUERY_TEST_TABLE_DDL);
    runHiveQuery("INSERT INTO " + TEST_TABLE_NAME + " VALUES (123, 'hello')");
    // Check that no events or links are created since we're not moving data from a known entity
    List<Link> links = getBigQueryLinks(null, TEST_TABLE_NAME);
    assertThat(links).isEmpty();
    List<LineageEvent> events = getDataplexEvents();
    assertThat(events).isEmpty();
  }

  @Test
  public void testCopyIntoBigQuery() throws InterruptedException, IOException {
    String localHiveTable = "myhivetable";
    createManagedHiveTable(localHiveTable, HIVE_TEST_TABLE_DDL);
    // Create a BigQuery table
    createExternalHiveBigQueryTable(TEST_TABLE_NAME, HIVE_TEST_TABLE_DDL, BIGQUERY_TEST_TABLE_DDL);
    // Write from the local table to the BigQuery table
    runHiveQuery(
        String.format(
            "INSERT INTO %s SELECT number, text FROM %s", TEST_TABLE_NAME, localHiveTable));
    // Check that the Dataplex links were correctly created
    List<Link> links = getBigQueryLinks(null, TEST_TABLE_NAME);
    assertThat(links).hasSize(1);
    assertThat(links.get(0).getSource().getFullyQualifiedName()).startsWith("filesystem:");
    assertThat(links.get(0).getSource().getFullyQualifiedName()).endsWith("/" + localHiveTable);
    assertThat(links.get(0).getTarget().getFullyQualifiedName())
        .isEqualTo(String.format("bigquery:%s.%s.%s", getProject(), dataset, TEST_TABLE_NAME));
  }

  @Test
  public void testGcsTableToBigQueryTable() throws InterruptedException, IOException {
    // Upload CSV to the GCS bucket
    uploadBlob(
        testBucketName,
        "inputs/" + TEST_TABLE_NAME + "/part_000000.csv",
        "number,text\n123,hello\n456,hola".getBytes(StandardCharsets.UTF_8));
    // Create the external Hive table backed by the CSV file
    createGcsCsvTable(
        TEST_TABLE_NAME,
        HIVE_TEST_TABLE_DDL,
        "gs://" + testBucketName + "/inputs/" + TEST_TABLE_NAME,
        true);
    // Create a new BigQuery-backed Hive table
    createExternalHiveBigQueryTable(
        ANOTHER_TEST_TABLE_NAME, HIVE_TEST_TABLE_DDL, BIGQUERY_TEST_TABLE_DDL);
    // Write from the GCS-backed table to the BigQuery table
    runHiveQuery(
        String.format(
            "INSERT INTO %s SELECT number, text FROM %s",
            ANOTHER_TEST_TABLE_NAME, TEST_TABLE_NAME));
    // Check that the Dataplex links were correctly created
    List<Link> links = getBigQueryLinks(null, ANOTHER_TEST_TABLE_NAME);
    assertThat(links).hasSize(1);
    assertThat(links.get(0).getSource().getFullyQualifiedName())
        .isEqualTo(String.format("gcs:%s.inputs/%s", testBucketName, TEST_TABLE_NAME));
    assertThat(links.get(0).getTarget().getFullyQualifiedName())
        .isEqualTo(
            String.format("bigquery:%s.%s.%s", getProject(), dataset, ANOTHER_TEST_TABLE_NAME));
  }
}
