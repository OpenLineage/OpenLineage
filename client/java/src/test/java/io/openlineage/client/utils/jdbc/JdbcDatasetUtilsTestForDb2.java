/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.utils.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Properties;
import org.junit.jupiter.api.Test;

@SuppressWarnings("PMD.AvoidUsingHardCodedIP")
class JdbcDatasetUtilsTestForDb2 {
  @Test
  void testGetDatasetIdentifierWithHost() {
    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:db2://test-host.com", "schema.table1", new Properties()))
        .hasFieldOrPropertyWithValue("namespace", "db2://test-host.com:6789")
        .hasFieldOrPropertyWithValue("name", "schema.table1");
  }

  @Test
  void testGetDatasetIdentifierWithIP() {
    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:db2://198.51.100.1", "schema.table1", new Properties()))
        .hasFieldOrPropertyWithValue("namespace", "db2://198.51.100.1:6789")
        .hasFieldOrPropertyWithValue("name", "schema.table1");
  }
}
