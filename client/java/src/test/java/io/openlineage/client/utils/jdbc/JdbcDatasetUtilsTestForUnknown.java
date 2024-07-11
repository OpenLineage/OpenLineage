/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.utils.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Properties;
import org.junit.jupiter.api.Test;

@SuppressWarnings("PMD.AvoidUsingHardCodedIP")
class JdbcDatasetUtilsTestForUnknown {
  @Test
  void testGetDatasetIdentifierWithHost() {
    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:unknown://test-host.com", "schema.table1", new Properties()))
        .hasFieldOrPropertyWithValue("namespace", "unknown://test-host.com")
        .hasFieldOrPropertyWithValue("name", "schema.table1");
  }

  @Test
  void testGetDatasetIdentifierWithIPv4() {
    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:unknown://192.168.1.1", "schema.table1", new Properties()))
        .hasFieldOrPropertyWithValue("namespace", "unknown://192.168.1.1")
        .hasFieldOrPropertyWithValue("name", "schema.table1");
  }

  @Test
  void testGetDatasetIdentifierWithIPv6() {
    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:unknown://[3ffe:8311:eeee:f70f:0:5eae:10.203.31.9]",
                "schema.table1",
                new Properties()))
        .hasFieldOrPropertyWithValue(
            "namespace", "unknown://[3ffe:8311:eeee:f70f:0:5eae:10.203.31.9]")
        .hasFieldOrPropertyWithValue("name", "schema.table1");

    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:unknown://3ffe:8311:eeee:f70f:0:5eae:10.203.31.9",
                "schema.table1",
                new Properties()))
        .hasFieldOrPropertyWithValue(
            "namespace", "unknown://3ffe:8311:eeee:f70f:0:5eae:10.203.31.9")
        .hasFieldOrPropertyWithValue("name", "schema.table1");
  }

  @Test
  void testGetDatasetIdentifierWithCredentialsAsParams() {
    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:unknown://fred:sec%40ret@hostname", "schema.table1", new Properties()))
        .hasFieldOrPropertyWithValue("namespace", "unknown://hostname")
        .hasFieldOrPropertyWithValue("name", "schema.table1");

    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:unknown://hostname?user=fred&password=sec%40ret",
                "schema.table1", new Properties()))
        .hasFieldOrPropertyWithValue("namespace", "unknown://hostname")
        .hasFieldOrPropertyWithValue("name", "schema.table1");

    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:unknown://hostname?username=fred&password=sec%40ret",
                "schema.table1", new Properties()))
        .hasFieldOrPropertyWithValue("namespace", "unknown://hostname")
        .hasFieldOrPropertyWithValue("name", "schema.table1");

    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:unknown://hostname:user=fred;password=sec%40ret",
                "schema.table1", new Properties()))
        .hasFieldOrPropertyWithValue("namespace", "unknown://hostname")
        .hasFieldOrPropertyWithValue("name", "schema.table1");

    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:unknown://hostname:username=fred;password=sec%40ret",
                "schema.table1", new Properties()))
        .hasFieldOrPropertyWithValue("namespace", "unknown://hostname")
        .hasFieldOrPropertyWithValue("name", "schema.table1");

    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:unknown://hostname;user=fred;password=sec%40ret;",
                "schema.table1", new Properties()))
        .hasFieldOrPropertyWithValue("namespace", "unknown://hostname")
        .hasFieldOrPropertyWithValue("name", "schema.table1");

    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:unknown://hostname;username=fred;password=sec%40ret;",
                "schema.table1", new Properties()))
        .hasFieldOrPropertyWithValue("namespace", "unknown://hostname")
        .hasFieldOrPropertyWithValue("name", "schema.table1");
  }

  @Test
  void testGetDatasetIdentifierWithPort() {
    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:unknown://hostname:5432", "schema.table1", new Properties()))
        .hasFieldOrPropertyWithValue("namespace", "unknown://hostname:5432")
        .hasFieldOrPropertyWithValue("name", "schema.table1");
  }

  @Test
  void testGetDatasetIdentifierWithDatabase() {
    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:unknown://hostname/mydb", "schema.table1", new Properties()))
        .hasFieldOrPropertyWithValue("namespace", "unknown://hostname")
        .hasFieldOrPropertyWithValue("name", "mydb.schema.table1");
  }

  @Test
  void testGetDatasetIdentifierWithExtraProperties() {
    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:unknown://hostname?ssl=true&applicationName=MyApp",
                "schema.table1",
                new Properties()))
        .hasFieldOrPropertyWithValue("namespace", "unknown://hostname")
        .hasFieldOrPropertyWithValue("name", "schema.table1");

    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:unknown://hostname:ssl=true;applicationName=MyApp",
                "schema.table1",
                new Properties()))
        .hasFieldOrPropertyWithValue(
            "namespace", "unknown://hostname:ssl=true;applicationName=MyApp")
        .hasFieldOrPropertyWithValue("name", "schema.table1");

    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:unknown://hostname;ssl=true;applicationName=MyApp;",
                "schema.table1",
                new Properties()))
        .hasFieldOrPropertyWithValue(
            "namespace", "unknown://hostname;ssl=true;applicationName=MyApp;")
        .hasFieldOrPropertyWithValue("name", "schema.table1");
  }

  @Test
  void testGetDatasetIdentifierWithUppercaseUrl() {
    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "JDBC:UNKNOWN://TEST.HOST.COM/MYDB", "SCHEMA.TABLE1", new Properties()))
        .hasFieldOrPropertyWithValue("namespace", "unknown://test.host.com")
        .hasFieldOrPropertyWithValue("name", "MYDB.SCHEMA.TABLE1");
  }

  @Test
  void testGetDatasetIdentifierWithMultipleHosts() {
    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:unknown://hostname1,hostname2", "schema.table1", new Properties()))
        .hasFieldOrPropertyWithValue("namespace", "unknown://hostname1,hostname2")
        .hasFieldOrPropertyWithValue("name", "schema.table1");

    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:unknown://hostname1:5432,hostname2:5432", "schema.table1", new Properties()))
        .hasFieldOrPropertyWithValue("namespace", "unknown://hostname1:5432,hostname2:5432")
        .hasFieldOrPropertyWithValue("name", "schema.table1");

    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:unknown://fred:sec%40ret@hostname1:5432,fred:sec%40ret@hostname2:5432",
                "schema.table1", new Properties()))
        .hasFieldOrPropertyWithValue("namespace", "unknown://hostname1:5432,hostname2:5432")
        .hasFieldOrPropertyWithValue("name", "schema.table1");
  }
}
