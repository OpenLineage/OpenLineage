/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.utils.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Properties;
import org.junit.jupiter.api.Test;

@SuppressWarnings("PMD.AvoidUsingHardCodedIP")
class JdbcDatasetUtilsTestForPostgres {
  @Test
  void testGetDatasetIdentifierWithHost() {
    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:postgresql://test.host.com", "schema.table1", new Properties()))
        .hasFieldOrPropertyWithValue("namespace", "postgres://test.host.com")
        .hasFieldOrPropertyWithValue("name", "schema.table1");
  }

  @Test
  void testGetDatasetIdentifierWithIPv4() {
    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:postgresql://192.168.1.1", "schema.table1", new Properties()))
        .hasFieldOrPropertyWithValue("namespace", "postgres://192.168.1.1")
        .hasFieldOrPropertyWithValue("name", "schema.table1");
  }

  @Test
  void testGetDatasetIdentifierWithIPv6() {
    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:postgresql://[3ffe:8311:eeee:f70f:0:5eae:10.203.31.9]",
                "schema.table1",
                new Properties()))
        .hasFieldOrPropertyWithValue(
            "namespace", "postgres://[3ffe:8311:eeee:f70f:0:5eae:10.203.31.9]")
        .hasFieldOrPropertyWithValue("name", "schema.table1");
  }

  @Test
  void testGetDatasetIdentifierWithCredentials() {
    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:postgresql://hostname?user=fred&password=sec%40ret",
                "schema.table1", new Properties()))
        .hasFieldOrPropertyWithValue("namespace", "postgres://hostname")
        .hasFieldOrPropertyWithValue("name", "schema.table1");
  }

  @Test
  void testGetDatasetIdentifierWithPort() {
    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:postgresql://hostname:5432", "schema.table1", new Properties()))
        .hasFieldOrPropertyWithValue("namespace", "postgres://hostname:5432")
        .hasFieldOrPropertyWithValue("name", "schema.table1");
  }

  @Test
  void testGetDatasetIdentifierWithDatabase() {
    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:postgresql://hostname/mydb", "schema.table1", new Properties()))
        .hasFieldOrPropertyWithValue("namespace", "postgres://hostname")
        .hasFieldOrPropertyWithValue("name", "mydb.schema.table1");
  }

  @Test
  void testGetDatasetIdentifierWithExtraProperties() {
    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:postgresql://hostname?ssl=true&applicationName=MyApp",
                "schema.table1",
                new Properties()))
        .hasFieldOrPropertyWithValue("namespace", "postgres://hostname")
        .hasFieldOrPropertyWithValue("name", "schema.table1");
  }

  @Test
  void testGetDatasetIdentifierWithMultipleHosts() {
    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:postgresql://hostname1,hostname2", "schema.table1", new Properties()))
        .hasFieldOrPropertyWithValue("namespace", "postgres://hostname1,hostname2")
        .hasFieldOrPropertyWithValue("name", "schema.table1");

    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:postgresql://hostname1:5432,hostname2:5432",
                "schema.table1",
                new Properties()))
        .hasFieldOrPropertyWithValue("namespace", "postgres://hostname1:5432,hostname2:5432")
        .hasFieldOrPropertyWithValue("name", "schema.table1");
  }
}
