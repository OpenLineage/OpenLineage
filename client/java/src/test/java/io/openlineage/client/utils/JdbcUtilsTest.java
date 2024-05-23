/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.utils;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class JdbcUtilsTest {

  @Test
  void testSanitizeJdbc() {
    assertThat(JdbcUtils.sanitizeJdbcUrl("postgres://localhost:5432"))
        .isEqualTo("postgres://localhost:5432");

    assertThat(JdbcUtils.sanitizeJdbcUrl("jdbc:postgresql://localhost:5432"))
        .isEqualTo("postgres://localhost:5432");

    assertThat(
            JdbcUtils.sanitizeJdbcUrl(
                "jdbc:postgresql://localhost:5432?user=postgres&password=postgres"))
        .isEqualTo("postgres://localhost:5432");

    assertThat(
            JdbcUtils.sanitizeJdbcUrl(
                "jdbc:postgresql://localhost:5432?username=postgres&password=postgres"))
        .isEqualTo("postgres://localhost:5432");
  }

  @Test
  void testGetDatasetIdentifierFromJdbcUrl() {
    assertThat(
            JdbcUtils.getDatasetIdentifierFromJdbcUrl("jdbc:postgresql://localhost:5432/", "table"))
        .hasFieldOrPropertyWithValue("namespace", "postgres://localhost:5432")
        .hasFieldOrPropertyWithValue("name", "table");

    assertThat(
            JdbcUtils.getDatasetIdentifierFromJdbcUrl(
                "jdbc:postgresql://localhost:5432/db", "table"))
        .hasFieldOrPropertyWithValue("namespace", "postgres://localhost:5432")
        .hasFieldOrPropertyWithValue("name", "db.table");
  }
}
