/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.utils.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Properties;
import org.junit.jupiter.api.Test;

@SuppressWarnings("PMD.AvoidUsingHardCodedIP")
class JdbcDatasetUtilsTestForCassandra {
  @Test
  void testGetDatasetIdentifierWithHost() {
    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:cassandra://test-host.com", "keyspace.table1", new Properties()))
        .hasFieldOrPropertyWithValue("namespace", "cassandra://test-host.com:9042")
        .hasFieldOrPropertyWithValue("name", "keyspace.table1");
  }

  @Test
  void testGetDatasetIdentifierWithPort() {
    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:cassandra://test-host.com:9043", "keyspace.table1", new Properties()))
        .hasFieldOrPropertyWithValue("namespace", "cassandra://test-host.com:9043")
        .hasFieldOrPropertyWithValue("name", "keyspace.table1");
  }

  @Test
  void testGetDatasetIdentifierWithKeyspace() {
    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:cassandra://test-host.com:9042/keyspace", "table1", new Properties()))
        .hasFieldOrPropertyWithValue("namespace", "cassandra://test-host.com:9042")
        .hasFieldOrPropertyWithValue("name", "keyspace.table1");
  }

  @Test
  void testGetDatasetIdentifierWithIPv4() {
    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:cassandra://192.168.1.1", "keyspace.table1", new Properties()))
        .hasFieldOrPropertyWithValue("namespace", "cassandra://192.168.1.1:9042")
        .hasFieldOrPropertyWithValue("name", "keyspace.table1");
  }

  @Test
  void testGetDatasetIdentifierWithIPv6() {
    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:cassandra://[2001:db8::1]", "keyspace.table1", new Properties()))
        .hasFieldOrPropertyWithValue("namespace", "cassandra://[2001:db8::1]:9042")
        .hasFieldOrPropertyWithValue("name", "keyspace.table1");
  }

  @Test
  void testGetDatasetIdentifierWithMixedHosts() {
    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:cassandra://[::1]--127.0.0.1:9042--host1/keyspace",
                "table1",
                new Properties()))
        .hasFieldOrPropertyWithValue(
            "namespace", "cassandra://[::1]:9042,127.0.0.1:9042,host1:9042")
        .hasFieldOrPropertyWithValue("name", "keyspace.table1");
  }
}
