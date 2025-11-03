/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.utils.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Properties;
import org.junit.jupiter.api.Test;

@SuppressWarnings("PMD.AvoidUsingHardCodedIP")
class JdbcDatasetUtilsTestForSqlServerJtds {
  @Test
  void testGetDatasetIdentifierWithHost() {
    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:jtds:sqlserver://test-host.com", "schema.table1", new Properties()))
        .hasFieldOrPropertyWithValue("namespace", "sqlserver://test-host.com")
        .hasFieldOrPropertyWithValue("name", "schema.table1");
  }

  @Test
  void testGetDatasetIdentifierWithIPv4() {
    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:jtds:sqlserver://192.168.1.1", "schema.table1", new Properties()))
        .hasFieldOrPropertyWithValue("namespace", "sqlserver://192.168.1.1")
        .hasFieldOrPropertyWithValue("name", "schema.table1");
  }

  @Test
  void testGetDatasetIdentifierWithCredentials() {
    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:jtds:sqlserver://hostname;user=fred;password=sec%40ret",
                "schema.table1", new Properties()))
        .hasFieldOrPropertyWithValue("namespace", "sqlserver://hostname")
        .hasFieldOrPropertyWithValue("name", "schema.table1");
  }

  @Test
  void testGetDatasetIdentifierWithDatabase() {
    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:jtds:sqlserver://hostname/MYTESTDB", "schema.table1", new Properties()))
        .hasFieldOrPropertyWithValue("namespace", "sqlserver://hostname")
        .hasFieldOrPropertyWithValue("name", "MYTESTDB.schema.table1");
  }

  @Test
  void testGetDatasetIdentifierWithPort() {
    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:jtds:sqlserver://hostname:1433", "schema.table1", new Properties()))
        .hasFieldOrPropertyWithValue("namespace", "sqlserver://hostname:1433")
        .hasFieldOrPropertyWithValue("name", "schema.table1");
  }

  @Test
  void testGetDatasetIdentifierWithInstance() {
    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:jtds:sqlserver://hostname;instance=someinstance",
                "schema.table1",
                new Properties()))
        .hasFieldOrPropertyWithValue("namespace", "sqlserver://hostname/someinstance")
        .hasFieldOrPropertyWithValue("name", "schema.table1");

    Properties props = new Properties();
    props.setProperty("instance", "someinstance");
    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:jtds:sqlserver://hostname", "schema.table1", props))
        .hasFieldOrPropertyWithValue("namespace", "sqlserver://hostname/someinstance")
        .hasFieldOrPropertyWithValue("name", "schema.table1");
  }

  @Test
  void testGetDatasetIdentifierWithExtraProperties() {
    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:jtds:sqlserver://hostname;autoCommit=true;appName=MyApp;",
                "schema.table1",
                new Properties()))
        .hasFieldOrPropertyWithValue("namespace", "sqlserver://hostname")
        .hasFieldOrPropertyWithValue("name", "schema.table1");
  }

  @Test
  void testGetDatasetIdentifierWithUppercaseUrl() {
    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jDBC:JTDS:SQLSERVER://TEST.HOST.COM/MYDB", "SCHEMA.TABLE1", new Properties()))
        .hasFieldOrPropertyWithValue("namespace", "sqlserver://test.host.com")
        .hasFieldOrPropertyWithValue("name", "MYDB.SCHEMA.TABLE1");
  }
}
