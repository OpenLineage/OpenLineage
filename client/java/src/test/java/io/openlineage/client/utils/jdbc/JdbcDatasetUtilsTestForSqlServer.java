/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.utils.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Properties;
import org.junit.jupiter.api.Test;

@SuppressWarnings("PMD.AvoidUsingHardCodedIP")
class JdbcDatasetUtilsTestForSqlServer {
  @Test
  void testGetDatasetIdentifierWithHost() {
    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:sqlserver://test.host.com", "schema.table1", new Properties()))
        .hasFieldOrPropertyWithValue("namespace", "sqlserver://test.host.com")
        .hasFieldOrPropertyWithValue("name", "schema.table1");
  }

  @Test
  void testGetDatasetIdentifierWithIPv4() {
    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:sqlserver://192.168.1.1", "schema.table1", new Properties()))
        .hasFieldOrPropertyWithValue("namespace", "sqlserver://192.168.1.1")
        .hasFieldOrPropertyWithValue("name", "schema.table1");
  }

  @Test
  void testGetDatasetIdentifierWithCredentials() {
    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:sqlserver://hostname;user=fred;password=sec%40ret",
                "schema.table1", new Properties()))
        .hasFieldOrPropertyWithValue("namespace", "sqlserver://hostname")
        .hasFieldOrPropertyWithValue("name", "schema.table1");
  }

  @Test
  void testGetDatasetIdentifierWithDatabase() {
    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:sqlserver://hostname;database=MYTESTDB", "schema.table1", new Properties()))
        .hasFieldOrPropertyWithValue("namespace", "sqlserver://hostname")
        .hasFieldOrPropertyWithValue("name", "MYTESTDB.schema.table1");

    Properties props = new Properties();
    props.setProperty("database", "MYTESTDB");
    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:sqlserver://hostname", "schema.table1", props))
        .hasFieldOrPropertyWithValue("namespace", "sqlserver://hostname")
        .hasFieldOrPropertyWithValue("name", "MYTESTDB.schema.table1");
  }

  @Test
  void testGetDatasetIdentifierWithDatabaseName() {
    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:sqlserver://hostname;databaseName=MYTESTDB",
                "schema.table1",
                new Properties()))
        .hasFieldOrPropertyWithValue("namespace", "sqlserver://hostname")
        .hasFieldOrPropertyWithValue("name", "MYTESTDB.schema.table1");

    Properties props = new Properties();
    props.setProperty("databaseName", "MYTESTDB");
    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:sqlserver://hostname", "schema.table1", props))
        .hasFieldOrPropertyWithValue("namespace", "sqlserver://hostname")
        .hasFieldOrPropertyWithValue("name", "MYTESTDB.schema.table1");
  }

  @Test
  void testGetDatasetIdentifierWithPort() {
    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:sqlserver://hostname:1433", "schema.table1", new Properties()))
        .hasFieldOrPropertyWithValue("namespace", "sqlserver://hostname:1433")
        .hasFieldOrPropertyWithValue("name", "schema.table1");
  }

  @Test
  void testGetDatasetIdentifierWithPortNumber() {
    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:sqlserver://hostname;portNumber=1433", "schema.table1", new Properties()))
        .hasFieldOrPropertyWithValue("namespace", "sqlserver://hostname:1433")
        .hasFieldOrPropertyWithValue("name", "schema.table1");

    Properties props = new Properties();
    props.setProperty("portNumber", "1433");
    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:sqlserver://hostname", "schema.table1", props))
        .hasFieldOrPropertyWithValue("namespace", "sqlserver://hostname:1433")
        .hasFieldOrPropertyWithValue("name", "schema.table1");
  }

  @Test
  void testGetDatasetIdentifierWithInstance() {
    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:sqlserver://hostname\\someinstance", "schema.table1", new Properties()))
        .hasFieldOrPropertyWithValue("namespace", "sqlserver://hostname/someinstance")
        .hasFieldOrPropertyWithValue("name", "schema.table1");
  }

  @Test
  void testGetDatasetIdentifierWithInstanceName() {
    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:sqlserver://hostname;instanceName=someinstance",
                "schema.table1",
                new Properties()))
        .hasFieldOrPropertyWithValue("namespace", "sqlserver://hostname/someinstance")
        .hasFieldOrPropertyWithValue("name", "schema.table1");

    Properties props = new Properties();
    props.setProperty("instanceName", "someinstance");
    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:sqlserver://hostname", "schema.table1", props))
        .hasFieldOrPropertyWithValue("namespace", "sqlserver://hostname/someinstance")
        .hasFieldOrPropertyWithValue("name", "schema.table1");
  }

  @Test
  void testGetDatasetIdentifierWithServerName() {
    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:sqlserver://;serverName=someServer", "schema.table1", new Properties()))
        .hasFieldOrPropertyWithValue("namespace", "sqlserver://someServer")
        .hasFieldOrPropertyWithValue("name", "schema.table1");

    Properties props = new Properties();
    props.setProperty("serverName", "someServer");
    assertThat(JdbcDatasetUtils.getDatasetIdentifier("jdbc:sqlserver://", "schema.table1", props))
        .hasFieldOrPropertyWithValue("namespace", "sqlserver://someServer")
        .hasFieldOrPropertyWithValue("name", "schema.table1");
  }

  @Test
  void testGetDatasetIdentifierWithServerNameIPv6() {
    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:sqlserver://;serverName=3ffe:8311:eeee:f70f:0:5eae:10.203.31.9",
                "schema.table1",
                new Properties()))
        .hasFieldOrPropertyWithValue(
            "namespace", "sqlserver://[3ffe:8311:eeee:f70f:0:5eae:10.203.31.9]")
        .hasFieldOrPropertyWithValue("name", "schema.table1");

    Properties props = new Properties();
    props.setProperty("serverName", "3ffe:8311:eeee:f70f:0:5eae:10.203.31.9");
    assertThat(JdbcDatasetUtils.getDatasetIdentifier("jdbc:sqlserver://", "schema.table1", props))
        .hasFieldOrPropertyWithValue(
            "namespace", "sqlserver://[3ffe:8311:eeee:f70f:0:5eae:10.203.31.9]")
        .hasFieldOrPropertyWithValue("name", "schema.table1");
  }

  @Test
  void testGetDatasetIdentifierWithExtraProperties() {
    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:sqlserver://hostname;integratedSecurity=true;applicationName=MyApp;",
                "schema.table1",
                new Properties()))
        .hasFieldOrPropertyWithValue("namespace", "sqlserver://hostname")
        .hasFieldOrPropertyWithValue("name", "schema.table1");
  }
}
