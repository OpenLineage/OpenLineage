/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.utils.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Properties;
import org.junit.jupiter.api.Test;

@SuppressWarnings("PMD.AvoidUsingHardCodedIP")
class JdbcDatasetUtilsTestForTeradata {
  @Test
  void testGetDatasetIdentifierWithHost() {
    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:teradata://test-host.com", "schema.table1", new Properties()))
        .hasFieldOrPropertyWithValue("namespace", "teradata://test-host.com:1025")
        .hasFieldOrPropertyWithValue("name", "schema.table1");
  }

  @Test
  void testGetDatasetIdentifierWithIPv4() {
    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:teradata://192.168.1.1", "schema.table1", new Properties()))
        .hasFieldOrPropertyWithValue("namespace", "teradata://192.168.1.1:1025")
        .hasFieldOrPropertyWithValue("name", "schema.table1");
  }

  @Test
  void testGetDatasetIdentifierWithIPv6() {
    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:teradata://[3ffe:8311:eeee:f70f:0:5eae:10.203.31.9]",
                "schema.table1",
                new Properties()))
        .hasFieldOrPropertyWithValue(
            "namespace", "teradata://[3ffe:8311:eeee:f70f:0:5eae:10.203.31.9]:1025")
        .hasFieldOrPropertyWithValue("name", "schema.table1");
  }

  @Test
  void testGetDatasetIdentifierWithCredentials() {
    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:teradata://hostname/USER=fred,PASSWORD=sec%40ret",
                "schema.table1", new Properties()))
        .hasFieldOrPropertyWithValue("namespace", "teradata://hostname:1025")
        .hasFieldOrPropertyWithValue("name", "schema.table1");
  }

  @Test
  void testGetDatasetIdentifierWithCustomPort() {
    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:teradata://hostname/DBS_PORT=1111", "schema.table1", new Properties()))
        .hasFieldOrPropertyWithValue("namespace", "teradata://hostname:1111")
        .hasFieldOrPropertyWithValue("name", "schema.table1");
  }

  @Test
  void testGetDatasetIdentifierWithDatabase() {
    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:teradata://hostname/DATABASE=mydb", "schema.table1", new Properties()))
        .hasFieldOrPropertyWithValue("namespace", "teradata://hostname:1025")
        .hasFieldOrPropertyWithValue("name", "mydb.schema.table1");
  }

  @Test
  void testGetDatasetIdentifierWithExtraProperties() {
    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:teradata://hostname/TMODE=TERADATA,APPNAME=MyApp",
                "schema.table1",
                new Properties()))
        .hasFieldOrPropertyWithValue("namespace", "teradata://hostname:1025")
        .hasFieldOrPropertyWithValue("name", "schema.table1");
  }

  @Test
  void testGetDatasetIdentifierWithUppercaseUrl() {
    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "JDBC:TERADATA://TEST.HOST.COM/DATABASE=MYDB", "SCHEMA.TABLE1", new Properties()))
        .hasFieldOrPropertyWithValue("namespace", "teradata://test.host.com:1025")
        .hasFieldOrPropertyWithValue("name", "MYDB.SCHEMA.TABLE1");
  }
}
