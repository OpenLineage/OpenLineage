/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.job;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class NamingTest {

  private static final String ENV_VAR = "OPENLINEAGE__NAME__ESCAPING";

  @SuppressWarnings({"unchecked", "PMD"})
  private void setEnv(Map<String, String> vars) throws Exception {
    Class<?> cls = System.getenv().getClass();
    Field f = cls.getDeclaredField("m");
    f.setAccessible(true);
    Map<String, String> writable = (Map<String, String>) f.get(System.getenv());
    writable.putAll(vars);
  }

  @SuppressWarnings({"unchecked", "PMD"})
  private void clearEnv(Set<String> keys) throws Exception {
    Class<?> cls = System.getenv().getClass();
    Field f = cls.getDeclaredField("m");
    f.setAccessible(true);
    Map<String, String> writable = (Map<String, String>) f.get(System.getenv());
    keys.forEach(writable::remove);
  }

  @AfterEach
  void restoreEnv() throws Exception {
    clearEnv(Set.of(ENV_VAR));
  }

  // -----------------------------------------------------------------------
  // Escaping disabled (default)
  // -----------------------------------------------------------------------

  @Test
  void sparkNameAllSegments_noEscaping() throws Exception {
    clearEnv(Set.of(ENV_VAR));
    Naming.Spark spark =
        Naming.Spark.builder()
            .appName("my.app")
            .command("execute_insert")
            .table("mydb.myschema.mytable")
            .build();

    assertThat(spark.getName()).isEqualTo("my.app.execute_insert.mydb.myschema.mytable");
  }

  @Test
  void sparkNameNullCommandAndTable_noEscaping() throws Exception {
    clearEnv(Set.of(ENV_VAR));
    Naming.Spark spark = Naming.Spark.builder().appName("my.app").build();

    assertThat(spark.getName()).isEqualTo("my.app");
  }

  @Test
  void sparkNameNullTable_noEscaping() throws Exception {
    clearEnv(Set.of(ENV_VAR));
    Naming.Spark spark = Naming.Spark.builder().appName("my.app").command("execute_insert").build();

    assertThat(spark.getName()).isEqualTo("my.app.execute_insert");
  }

  // -----------------------------------------------------------------------
  // Escaping enabled via environment variable
  // -----------------------------------------------------------------------

  @Test
  void sparkNameAllSegments_escapingEnabled() throws Exception {
    Map<String, String> env = new HashMap<>();
    env.put(ENV_VAR, "true");
    setEnv(env);

    try {
      Naming.Spark spark =
          Naming.Spark.builder()
              .appName("my.app")
              .command("execute_insert")
              .table("mydb.myschema.mytable")
              .build();

      // Each segment's internal dots are escaped; structural dots between segments are not.
      assertThat(spark.getName()).isEqualTo("my\\.app.execute_insert.mydb\\.myschema\\.mytable");
    } finally {
      clearEnv(env.keySet());
    }
  }

  @Test
  void sparkNameNullCommandAndTable_escapingEnabled() throws Exception {
    Map<String, String> env = new HashMap<>();
    env.put(ENV_VAR, "true");
    setEnv(env);

    try {
      Naming.Spark spark = Naming.Spark.builder().appName("my.app").build();

      assertThat(spark.getName()).isEqualTo("my\\.app");
    } finally {
      clearEnv(env.keySet());
    }
  }

  @Test
  void sparkNamePlainSegments_escapingEnabled_unchanged() throws Exception {
    Map<String, String> env = new HashMap<>();
    env.put(ENV_VAR, "true");
    setEnv(env);

    try {
      Naming.Spark spark =
          Naming.Spark.builder()
              .appName("myapp")
              .command("execute_insert")
              .table("mytable")
              .build();

      assertThat(spark.getName()).isEqualTo("myapp.execute_insert.mytable");
    } finally {
      clearEnv(env.keySet());
    }
  }
}
