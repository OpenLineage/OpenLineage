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
  void testSQLJobName() {
    Naming.SQL sql = Naming.SQL.builder().schema("gx").table("validate_datasets").build();

    assertEquals("gx.validate_datasets", sql.getName());
  }

  @Test
  void testSparkEmptyFieldsThrowException() {
    assertThrows(IllegalArgumentException.class, () -> new Naming.Spark("", "cmd", "table"));

    assertThrows(IllegalArgumentException.class, () -> new Naming.Spark("app", "", "table"));

    assertThrows(IllegalArgumentException.class, () -> new Naming.Spark("app", "cmd", ""));
  }

  @Test
  void testSQLEmptyFieldsThrowException() {
    assertThrows(IllegalArgumentException.class, () -> new Naming.SQL("", "table"));

    assertThrows(IllegalArgumentException.class, () -> new Naming.SQL("schema", ""));
  }

  @Test
  void testSparkNullFieldsThrowException() {
    assertThrows(
        NullPointerException.class,
        () -> Naming.Spark.builder().appName(null).command("cmd").table("table").build());

    assertThrows(
        NullPointerException.class,
        () -> Naming.Spark.builder().appName("app").command(null).table("table").build());

    assertThrows(
        NullPointerException.class,
        () -> Naming.Spark.builder().appName("app").command("cmd").table(null).build());
  }

  @Test
  void testSQLNullFieldsThrowException() {
    assertThrows(
        NullPointerException.class, () -> Naming.SQL.builder().schema(null).table("table").build());

    assertThrows(
        NullPointerException.class,
        () -> Naming.SQL.builder().schema("schema").table(null).build());
  }
}
