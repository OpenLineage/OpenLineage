/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.utils.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Properties;
import org.junit.jupiter.api.Test;

@SuppressWarnings("PMD.AvoidUsingHardCodedIP")
class JdbcDatasetUtilsTestForMysql {
  // https://dev.mysql.com/doc/connector-j/en/connector-j-reference-jdbc-url-format.html

  @Test
  void testGetDatasetIdentifierWithHost() {
    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:mysql://test.host.com", "schema.table1", new Properties()))
        .hasFieldOrPropertyWithValue("namespace", "mysql://test.host.com")
        .hasFieldOrPropertyWithValue("name", "schema.table1");
  }

  @Test
  void testGetDatasetIdentifierWithIPv4() {
    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:mysql://192.168.1.1", "schema.table1", new Properties()))
        .hasFieldOrPropertyWithValue("namespace", "mysql://192.168.1.1")
        .hasFieldOrPropertyWithValue("name", "schema.table1");
  }

  @Test
  void testGetDatasetIdentifierWithIPv6() {
    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:mysql://[3ffe:8311:eeee:f70f:0:5eae:10.203.31.9]",
                "schema.table1",
                new Properties()))
        .hasFieldOrPropertyWithValue(
            "namespace", "mysql://[3ffe:8311:eeee:f70f:0:5eae:10.203.31.9]")
        .hasFieldOrPropertyWithValue("name", "schema.table1");
  }

  @Test
  void testGetDatasetIdentifierWithCredentials() {
    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:mysql://hostname?user=fred&password=sec%40ret",
                "schema.table1", new Properties()))
        .hasFieldOrPropertyWithValue("namespace", "mysql://hostname")
        .hasFieldOrPropertyWithValue("name", "schema.table1");

    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:mysql://fred:sec%40ret@hostname", "schema.table1", new Properties()))
        .hasFieldOrPropertyWithValue("namespace", "mysql://hostname")
        .hasFieldOrPropertyWithValue("name", "schema.table1");
  }

  @Test
  void testGetDatasetIdentifierWithPort() {
    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:mysql://hostname:1521", "schema.table1", new Properties()))
        .hasFieldOrPropertyWithValue("namespace", "mysql://hostname:1521")
        .hasFieldOrPropertyWithValue("name", "schema.table1");
  }

  @Test
  void testGetDatasetIdentifierWithDatabase() {
    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:mysql://hostname/mydb", "schema.table1", new Properties()))
        .hasFieldOrPropertyWithValue("namespace", "mysql://hostname")
        .hasFieldOrPropertyWithValue("name", "mydb.schema.table1");
  }

  @Test
  void testGetDatasetIdentifierWithExtraProperties() {
    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:mysql://hostname?ssl=true&applicationName=MyApp",
                "schema.table1",
                new Properties()))
        .hasFieldOrPropertyWithValue("namespace", "mysql://hostname")
        .hasFieldOrPropertyWithValue("name", "schema.table1");
  }

  @Test
  void testGetDatasetIdentifierWithMultipleHostsInSimpleFormat() {
    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:mysql://myhost1:1111,myhost2:2222", "schema.table1", new Properties()))
        .hasFieldOrPropertyWithValue("namespace", "mysql://myhost1:1111,myhost2:2222")
        .hasFieldOrPropertyWithValue("name", "schema.table1");

    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:mysql://username:pwd@myhost1:1111,username:pwd@myhost2:2222",
                "schema.table1",
                new Properties()))
        .hasFieldOrPropertyWithValue("namespace", "mysql://myhost1:1111,myhost2:2222")
        .hasFieldOrPropertyWithValue("name", "schema.table1");
  }

  @Test
  void testGetDatasetIdentifierWithMultipleHostsInAddressFormat() {
    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:mysql://address=(host=myhost1)(port=1111),address=(host=myhost2)(port=2222)",
                "schema.table1",
                new Properties()))
        .hasFieldOrPropertyWithValue(
            "namespace",
            "mysql://address=(host=myhost1)(port=1111),address=(host=myhost2)(port=2222)")
        .hasFieldOrPropertyWithValue("name", "schema.table1");

    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:mysql://address=(host=myhost1)(port=1111)(user=sandy)(password=sec%40ret),address=(host=myhost2)(port=2222)(user=finn)(password=secret)",
                "schema.table1", new Properties()))
        .hasFieldOrPropertyWithValue(
            "namespace",
            "mysql://address=(host=myhost1)(port=1111),address=(host=myhost2)(port=2222)")
        .hasFieldOrPropertyWithValue("name", "schema.table1");
  }

  @Test
  void testGetDatasetIdentifierWithMultipleHostsInKeyValueFormat() {
    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:mysql://(host=myhost1,port=1111),(host=myhost2,port=2222)",
                "schema.table1",
                new Properties()))
        .hasFieldOrPropertyWithValue(
            "namespace", "mysql://(host=myhost1,port=1111),(host=myhost2,port=2222)")
        .hasFieldOrPropertyWithValue("name", "schema.table1");

    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:mysql://(host=myhost1,port=1111,user=sandy,password=sec%40ret),(host=myhost2,port=2222,user=finn,password=secret)",
                "schema.table1", new Properties()))
        .hasFieldOrPropertyWithValue(
            "namespace", "mysql://(host=myhost1,port=1111),(host=myhost2,port=2222)")
        .hasFieldOrPropertyWithValue("name", "schema.table1");
  }
}
