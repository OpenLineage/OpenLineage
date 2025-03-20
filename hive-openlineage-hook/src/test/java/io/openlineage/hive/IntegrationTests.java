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
package io.openlineage.hive;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.TableResult;
import com.google.common.collect.Streams;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

public class IntegrationTests extends IntegrationTestsBase {

  public static final String TEST_TABLE_NAME = "mytable";
  public static final String ANOTHER_TEST_TABLE_NAME = "anothertable";
  public static final String HIVE_TEST_TABLE_DDL = "number INT, text STRING";
  public static String BIGQUERY_TEST_TABLE_DDL = String.join("\n", "number INT64,", "text STRING");

  @Test
  public void testSelectAll() {
    createManagedHiveTable(TEST_TABLE_NAME, HIVE_TEST_TABLE_DDL);
    runHiveQuery("SELECT * FROM " + TEST_TABLE_NAME);
  }

  @Test
  public void testInsert() {
    createManagedHiveTable(TEST_TABLE_NAME, HIVE_TEST_TABLE_DDL);
    runHiveQuery(String.format("INSERT INTO %s VALUES (99, 'hello')", TEST_TABLE_NAME));
  }

  @Test
  public void testAddColumn() {
    createManagedHiveTable(TEST_TABLE_NAME, HIVE_TEST_TABLE_DDL);
    runHiveQuery(String.format("ALTER TABLE %s ADD COLUMNS (fl FLOAT)", TEST_TABLE_NAME));
  }

  @Test
  public void testCTAS() {
    createManagedHiveTable("employees", "id int, name string, team int");
    runHiveQuery("insert into employees values (1, 'Tom', 1), (2, 'Jessie', 1), (3, 'Emy', 2)");
    createManagedHiveTable("managers", "id int, name string, team int");
    runHiveQuery("insert into managers values (1, 'Tony', 1), (2, 'Spencer', 2)");
    createManagedHiveTable("teams", "id int, type int, building string");
    runHiveQuery("insert into teams values (1, 1, 'D10'), (2, 2, 'D20')");
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
  }

  @Test
  public void testAnotherCTAS() {
    createManagedHiveTable("employees", "emp_id INT, emp_name STRING, dept_id INT, dob DATE");
    runHiveQuery(
        String.join(
            "\n",
            "INSERT INTO employees VALUES ",
            "(1, 'John Doe', 101, '1990-05-15'),",
            "(2, 'Jane Smith', 102, '1995-08-22'),",
            "(3, 'Sam Brown', 101, '1992-03-10')"));
    createManagedHiveTable("departments", "dept_id INT, dept_name STRING, location STRING");
    runHiveQuery(
        String.join(
            "\n",
            "INSERT INTO departments VALUES",
            "(101, 'Engineering', 'New York'),",
            "(102, 'Marketing', 'San Francisco')"));
    runHiveQuery(
        String.join(
            "\n",
            "CREATE TABLE employee_details AS",
            "SELECT ",
            "  e.emp_id,",
            "  e.emp_name,",
            "  d.dept_name,",
            "  d.location,",
            "  YEAR('2020-01-01') - YEAR(e.dob) AS age_in_2020,",
            "  CONCAT(e.emp_name, ' - ', d.dept_name) AS emp_dept_info",
            "FROM employees e",
            "JOIN departments d",
            "ON e.dept_id = d.dept_id;"));
  }

  @Test
  public void testBigQuery() {
    createExternalHiveBigQueryTable(TEST_TABLE_NAME, HIVE_TEST_TABLE_DDL, BIGQUERY_TEST_TABLE_DDL);
    // Run two insert queries using Hive
    runHiveQuery("INSERT INTO " + TEST_TABLE_NAME + " VALUES (123, 'hello')");
    runHiveQuery("INSERT INTO " + TEST_TABLE_NAME + " VALUES (789, 'abcd')");
    // Read the data using the BQ SDK
    TableResult result =
        runBqQuery(
            String.format(
                "SELECT number, text FROM `${dataset}.%s` ORDER BY number", TEST_TABLE_NAME));
    // Verify we get the expected values
    assertEquals(2, result.getTotalRows());
    List<FieldValueList> rows = Streams.stream(result.iterateAll()).collect(Collectors.toList());
    assertEquals(123L, rows.get(0).get(0).getLongValue());
    assertEquals("hello", rows.get(0).get(1).getStringValue());
    assertEquals(789L, rows.get(1).get(0).getLongValue());
    assertEquals("abcd", rows.get(1).get(1).getStringValue());
  }

  @Test
  public void testGcsTableToBigQueryTable() {
    // Upload CSV to the GCS bucket
    uploadBlob(
        getTestBucket(),
        "inputs/" + TEST_TABLE_NAME + "/part_000000.csv",
        "number,text\n123,hello\n456,hola".getBytes(StandardCharsets.UTF_8));
    // Create the external Hive table backed by the CSV file
    createGcsCsvTable(
        TEST_TABLE_NAME,
        HIVE_TEST_TABLE_DDL,
        "gs://" + getTestBucket() + "/inputs/" + TEST_TABLE_NAME,
        true);
    // Check that the data is in place
    List<Object[]> rows = runHiveQuery("SELECT number, text FROM " + TEST_TABLE_NAME);
    assertEquals(
        "123", rows.get(0)[0]); // FIXME: Figure out why this returns a string instead of an int
    assertEquals("hello", rows.get(0)[1]);
    assertEquals(
        "456", rows.get(1)[0]); // FIXME: Figure out why this returns a string instead of an int
    assertEquals("hola", rows.get(1)[1]);
    // Create a new BigQuery-backed Hive table
    createExternalHiveBigQueryTable(
        ANOTHER_TEST_TABLE_NAME, HIVE_TEST_TABLE_DDL, BIGQUERY_TEST_TABLE_DDL);
    // Write from the GCS-backed table to the BigQuery table
    runHiveQuery(
        String.format(
            "INSERT INTO %s SELECT number, text FROM %s",
            ANOTHER_TEST_TABLE_NAME, TEST_TABLE_NAME));
  }
}
