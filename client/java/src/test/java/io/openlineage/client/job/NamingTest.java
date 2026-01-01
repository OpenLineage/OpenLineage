/*
/* Copyright 2018-2026 contributors to the OpenLineage project
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
  void testSparkEmptyFieldsThrowException() {
    assertThrows(IllegalArgumentException.class, () -> new Naming.Spark("", "cmd", "table"));
  }

  @Test
  void testSparkNullFieldsThrowException() {
    assertThrows(
        NullPointerException.class,
        () -> Naming.Spark.builder().appName(null).command("cmd").table("table").build());
  }
}
