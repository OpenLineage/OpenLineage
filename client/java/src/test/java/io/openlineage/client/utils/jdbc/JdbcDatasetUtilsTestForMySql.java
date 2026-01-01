/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.utils.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Properties;
import org.junit.jupiter.api.Test;

@SuppressWarnings("PMD.AvoidUsingHardCodedIP")
class JdbcDatasetUtilsTestForMySql {
  @Test
  void testGetDatasetIdentifierWithHost() {
    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:mysql://test-host.com", "schema.table1", new Properties()))
        .hasFieldOrPropertyWithValue("namespace", "mysql://test-host.com:3306")
        .hasFieldOrPropertyWithValue("name", "schema.table1");
  }

  @Test
  void testGetDatasetIdentifierWithIPv4() {
    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:mysql://192.168.1.1", "schema.table1", new Properties()))
        .hasFieldOrPropertyWithValue("namespace", "mysql://192.168.1.1:3306")
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
            "namespace", "mysql://[3ffe:8311:eeee:f70f:0:5eae:10.203.31.9]:3306")
        .hasFieldOrPropertyWithValue("name", "schema.table1");
  }

  @Test
  void testGetDatasetIdentifierWithCredentials() {
    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:mysql://hostname?user=fred&password=sec%40ret",
                "schema.table1", new Properties()))
        .hasFieldOrPropertyWithValue("namespace", "mysql://hostname:3306")
        .hasFieldOrPropertyWithValue("name", "schema.table1");

    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:mysql://fred:sec%40ret@hostname", "schema.table1", new Properties()))
        .hasFieldOrPropertyWithValue("namespace", "mysql://hostname:3306")
        .hasFieldOrPropertyWithValue("name", "schema.table1");
  }

  @Test
  void testGetDatasetIdentifierWithCustomPort() {
    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:mysql://hostname:3307", "schema.table1", new Properties()))
        .hasFieldOrPropertyWithValue("namespace", "mysql://hostname:3307")
        .hasFieldOrPropertyWithValue("name", "schema.table1");
  }

  @Test
  void testGetDatasetIdentifierWithDatabase() {
    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:mysql://hostname/mydb", "schema.table1", new Properties()))
        .hasFieldOrPropertyWithValue("namespace", "mysql://hostname:3306")
        .hasFieldOrPropertyWithValue("name", "mydb.schema.table1");
  }

  @Test
  void testGetDatasetIdentifierWithExtraProperties() {
    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:mysql://hostname?ssl=true&applicationName=MyApp",
                "schema.table1",
                new Properties()))
        .hasFieldOrPropertyWithValue("namespace", "mysql://hostname:3306")
        .hasFieldOrPropertyWithValue("name", "schema.table1");
  }

  @Test
  void testGetDatasetIdentifierWithDNSSrv() {
    // failover
    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:mysql+srv://domain.com", "schema.table1", new Properties()))
        .hasFieldOrPropertyWithValue("namespace", "mysql://domain.com:3306")
        .hasFieldOrPropertyWithValue("name", "schema.table1");

    // load balance
    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:mysql+srv:loadbalance://domain.com", "schema.table1", new Properties()))
        .hasFieldOrPropertyWithValue("namespace", "mysql://domain.com:3306")
        .hasFieldOrPropertyWithValue("name", "schema.table1");

    // replication
    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:mysql+srv:loadbalance://domain.com", "schema.table1", new Properties()))
        .hasFieldOrPropertyWithValue("namespace", "mysql://domain.com:3306")
        .hasFieldOrPropertyWithValue("name", "schema.table1");

    // with credentials
    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:mysql+srv://username:pwd@domain.com:3307", "schema.table1", new Properties()))
        .hasFieldOrPropertyWithValue("namespace", "mysql://domain.com:3307")
        .hasFieldOrPropertyWithValue("name", "schema.table1");
  }

  @Test
  void testGetDatasetIdentifierWithUppercaseUrl() {
    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "JDBC:MYSQL://TEST.HOST.COM/MYDB", "SCHEMA.TABLE1", new Properties()))
        .hasFieldOrPropertyWithValue("namespace", "mysql://test.host.com:3306")
        .hasFieldOrPropertyWithValue("name", "MYDB.SCHEMA.TABLE1");
  }

  @Test
  void testGetDatasetIdentifierWithMultipleHostsInSimpleFormat() {
    // failover
    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:mysql://myhost1,myhost2", "schema.table1", new Properties()))
        .hasFieldOrPropertyWithValue("namespace", "mysql://myhost1:3306,myhost2:3306")
        .hasFieldOrPropertyWithValue("name", "schema.table1");

    // load balance
    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:mysql:loadbalance://myhost1,myhost2", "schema.table1", new Properties()))
        .hasFieldOrPropertyWithValue("namespace", "mysql://myhost1:3306,myhost2:3306")
        .hasFieldOrPropertyWithValue("name", "schema.table1");

    // replication
    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:mysql:loadbalance://myhost1,myhost2", "schema.table1", new Properties()))
        .hasFieldOrPropertyWithValue("namespace", "mysql://myhost1:3306,myhost2:3306")
        .hasFieldOrPropertyWithValue("name", "schema.table1");

    // with credentials
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
    // Not supported yet
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
    // Not supported yet
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
