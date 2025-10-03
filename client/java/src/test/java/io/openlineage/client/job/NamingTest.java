/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.job;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class NamingTest {

  @Test
  void testSparkJobName() {
    Naming.Spark spark =
        Naming.Spark.builder()
            .appName("my_awesome_app")
            .command("execute_insert_into_hive_table")
            .table("mydb_mytable")
            .build();

    assertEquals("my_awesome_app.execute_insert_into_hive_table.mydb_mytable", spark.getName());
  }

  @Test
  void testHiveJobName() {
    Naming.Hive hive =
        Naming.Hive.builder()
            .operationName("simple_test.execute_create_hive_table_as_select_command.default_t2")
            .build();
    assertEquals(
        "simple_test.execute_create_hive_table_as_select_command.default_t2", hive.getName());
  }

  @Test
  void testSparkEmptyFieldsThrowException() {
    assertThrows(IllegalArgumentException.class, () -> new Naming.Spark("", "cmd", "table"));
  }

  @Test
  void testSparkNullFieldsThrowException() {
    assertThrows(
        NullPointerException.class,
        () -> Naming.Spark.builder().appName(null).command("cmd").table("table").build());
  }

  @Test
  void testHiveEmptyFiledsThrowException() {
    assertThrows(IllegalArgumentException.class, () -> new Naming.Hive(""));
  }
}
