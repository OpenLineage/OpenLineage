/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.utils.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Properties;
import org.junit.jupiter.api.Test;

@SuppressWarnings("PMD.AvoidUsingHardCodedIP")
class JdbcDatasetUtilsTestForCrate {
  @Test
  void testGetDatasetIdentifierWithHost() {
    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:crate://test-host.com", "schema.table1", new Properties()))
        .hasFieldOrPropertyWithValue("namespace", "crate://test-host.com:5432")
        .hasFieldOrPropertyWithValue("name", "schema.table1");
  }

  @Test
  void testGetDatasetIdentifierWithMultipleHosts() {
    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:crate://test-host.com,test-host2.com", "schema.table1", new Properties()))
        .hasFieldOrPropertyWithValue("namespace", "crate://test-host.com:5432,test-host2.com:5432")
        .hasFieldOrPropertyWithValue("name", "schema.table1");
  }

  @Test
  void testGetDatasetIdentifierWithIP() {
    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:crate://198.51.100.1", "schema.table1", new Properties()))
        .hasFieldOrPropertyWithValue("namespace", "crate://198.51.100.1:5432")
        .hasFieldOrPropertyWithValue("name", "schema.table1");
  }

  @Test
  void testGetDatasetIdentifierWithMultipleIPs() {
    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:crate://198.51.100.1,198.51.100.2", "schema.table1", new Properties()))
        .hasFieldOrPropertyWithValue("namespace", "crate://198.51.100.1:5432,198.51.100.2:5432")
        .hasFieldOrPropertyWithValue("name", "schema.table1");
  }
}
