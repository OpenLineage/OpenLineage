/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.utils.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Properties;
import org.junit.jupiter.api.Test;

@SuppressWarnings("PMD.AvoidUsingHardCodedIP")
class JdbcDatasetUtilsTestForClickHouse {
  @Test
  void testGetDatasetIdentifierWithHost() {
    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:clickhouse://test-host.com", "schema.table1", new Properties()))
        .hasFieldOrPropertyWithValue("namespace", "clickhouse://test-host.com:8123")
        .hasFieldOrPropertyWithValue("name", "schema.table1");
  }

  @Test
  void testGetDatasetIdentifierWithPort() {
    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:clickhouse://test-host.com:9000", "schema.table1", new Properties()))
        .hasFieldOrPropertyWithValue("namespace", "clickhouse://test-host.com:9000")
        .hasFieldOrPropertyWithValue("name", "schema.table1");
  }

  @Test
  void testGetDatasetIdentifierWithDatabase() {
    // Qualified table name: explicit database overrides URL database
    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:clickhouse://test-host.com:8123/mydb", "schema.table1", new Properties()))
        .hasFieldOrPropertyWithValue("namespace", "clickhouse://test-host.com:8123")
        .hasFieldOrPropertyWithValue("name", "schema.table1");

    // Unqualified table name: URL database is used
    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:clickhouse://test-host.com:8123/mydb", "table1", new Properties()))
        .hasFieldOrPropertyWithValue("namespace", "clickhouse://test-host.com:8123")
        .hasFieldOrPropertyWithValue("name", "mydb.table1");
  }

  @Test
  void testGetDatasetIdentifierWithMultipleHosts() {
    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:clickhouse://test-host.com,test-host2.com",
                "schema.table1",
                new Properties()))
        .hasFieldOrPropertyWithValue(
            "namespace", "clickhouse://test-host.com:8123,test-host2.com:8123")
        .hasFieldOrPropertyWithValue("name", "schema.table1");
  }

  @Test
  void testGetDatasetIdentifierWithIP() {
    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:clickhouse://198.51.100.1", "schema.table1", new Properties()))
        .hasFieldOrPropertyWithValue("namespace", "clickhouse://198.51.100.1:8123")
        .hasFieldOrPropertyWithValue("name", "schema.table1");
  }

  @Test
  void testGetDatasetIdentifierWithMultipleIPs() {
    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:clickhouse://198.51.100.1,198.51.100.2", "schema.table1", new Properties()))
        .hasFieldOrPropertyWithValue(
            "namespace", "clickhouse://198.51.100.1:8123,198.51.100.2:8123")
        .hasFieldOrPropertyWithValue("name", "schema.table1");
  }

  @Test
  void testGetDatasetIdentifierWithIPv6() {
    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:clickhouse://[2001:db8::1]", "schema.table1", new Properties()))
        .hasFieldOrPropertyWithValue("namespace", "clickhouse://[2001:db8::1]:8123")
        .hasFieldOrPropertyWithValue("name", "schema.table1");
  }

  @Test
  void testGetDatasetIdentifierWithChScheme() {
    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:ch://test-host.com", "schema.table1", new Properties()))
        .hasFieldOrPropertyWithValue("namespace", "clickhouse://test-host.com:8123")
        .hasFieldOrPropertyWithValue("name", "schema.table1");
  }

  @Test
  void testGetDatasetIdentifierWithChSchemeAndPort() {
    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:ch://test-host.com:9000", "schema.table1", new Properties()))
        .hasFieldOrPropertyWithValue("namespace", "clickhouse://test-host.com:9000")
        .hasFieldOrPropertyWithValue("name", "schema.table1");
  }

  @Test
  void testGetDatasetIdentifierWithChSchemeAndDatabase() {
    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:ch://test-host.com:8123/mydb", "schema.table1", new Properties()))
        .hasFieldOrPropertyWithValue("namespace", "clickhouse://test-host.com:8123")
        .hasFieldOrPropertyWithValue("name", "schema.table1");
  }

  @Test
  void testGetDatasetIdentifierWithChHttpProtocol() {
    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:ch:http://test-host.com:8123", "schema.table1", new Properties()))
        .hasFieldOrPropertyWithValue("namespace", "clickhouse://test-host.com:8123")
        .hasFieldOrPropertyWithValue("name", "schema.table1");
  }

  @Test
  void testGetDatasetIdentifierWithChHttpsProtocol() {
    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:ch:https://test-host.com:8443", "schema.table1", new Properties()))
        .hasFieldOrPropertyWithValue("namespace", "clickhouse://test-host.com:8443")
        .hasFieldOrPropertyWithValue("name", "schema.table1");
  }

  @Test
  void testGetDatasetIdentifierWithChGrpcProtocol() {
    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:ch:grpc://test-host.com:9100", "schema.table1", new Properties()))
        .hasFieldOrPropertyWithValue("namespace", "clickhouse://test-host.com:9100")
        .hasFieldOrPropertyWithValue("name", "schema.table1");
  }
}
