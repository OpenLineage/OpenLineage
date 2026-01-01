/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.utils.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Properties;
import org.junit.jupiter.api.Test;

@SuppressWarnings("PMD.AvoidUsingHardCodedIP")
class JdbcDatasetUtilsTestForOceanBase {
  @Test
  void testGetDatasetIdentifierWithHost() {
    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:oceanbase://test-host.com", "schema.table1", new Properties()))
        .hasFieldOrPropertyWithValue("namespace", "oceanbase://test-host.com:2881")
        .hasFieldOrPropertyWithValue("name", "schema.table1");
  }

  @Test
  void testGetDatasetIdentifierWithHAModeHost() {
    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:oceanbase:hamode://test-host.com", "schema.table1", new Properties()))
        .hasFieldOrPropertyWithValue("namespace", "oceanbase://test-host.com:2881")
        .hasFieldOrPropertyWithValue("name", "schema.table1");
  }

  @Test
  void testGetDatasetIdentifierWithIP() {
    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:oceanbase://198.51.100.1", "schema.table1", new Properties()))
        .hasFieldOrPropertyWithValue("namespace", "oceanbase://198.51.100.1:2881")
        .hasFieldOrPropertyWithValue("name", "schema.table1");
  }
}
