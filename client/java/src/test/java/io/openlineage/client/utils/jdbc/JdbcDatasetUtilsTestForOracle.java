/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.utils.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Properties;
import org.junit.jupiter.api.Test;

@SuppressWarnings("PMD.AvoidUsingHardCodedIP")
class JdbcDatasetUtilsTestForOracle {
  @Test
  void testGetDatasetIdentifierWithHost() {
    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:oracle:thin:@//test-host.com", "schema.table1", new Properties()))
        .hasFieldOrPropertyWithValue("namespace", "oracle://test-host.com:1521")
        .hasFieldOrPropertyWithValue("name", "schema.table1");

    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:oracle:thin:@test-host.com", "schema.table1", new Properties()))
        .hasFieldOrPropertyWithValue("namespace", "oracle://test-host.com:1521")
        .hasFieldOrPropertyWithValue("name", "schema.table1");
  }

  @Test
  void testGetDatasetIdentifierWithIPv4() {
    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:oracle:thin:@//192.168.1.1", "schema.table1", new Properties()))
        .hasFieldOrPropertyWithValue("namespace", "oracle://192.168.1.1:1521")
        .hasFieldOrPropertyWithValue("name", "schema.table1");

    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:oracle:thin:@192.168.1.1", "schema.table1", new Properties()))
        .hasFieldOrPropertyWithValue("namespace", "oracle://192.168.1.1:1521")
        .hasFieldOrPropertyWithValue("name", "schema.table1");
  }

  @Test
  void testGetDatasetIdentifierWithIPv6() {
    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:oracle:thin:@//[3ffe:8311:eeee:f70f:0:5eae:10.203.31.9]",
                "schema.table1",
                new Properties()))
        .hasFieldOrPropertyWithValue(
            "namespace", "oracle://[3ffe:8311:eeee:f70f:0:5eae:10.203.31.9]:1521")
        .hasFieldOrPropertyWithValue("name", "schema.table1");

    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:oracle:thin:@[3ffe:8311:eeee:f70f:0:5eae:10.203.31.9]",
                "schema.table1",
                new Properties()))
        .hasFieldOrPropertyWithValue(
            "namespace", "oracle://[3ffe:8311:eeee:f70f:0:5eae:10.203.31.9]:1521")
        .hasFieldOrPropertyWithValue("name", "schema.table1");
  }

  @Test
  void testGetDatasetIdentifierWithCredentials() {
    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:oracle:thin:user/password@//hostname", "schema.table1", new Properties()))
        .hasFieldOrPropertyWithValue("namespace", "oracle://hostname:1521")
        .hasFieldOrPropertyWithValue("name", "schema.table1");

    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:oracle:thin:user/password@hostname", "schema.table1", new Properties()))
        .hasFieldOrPropertyWithValue("namespace", "oracle://hostname:1521")
        .hasFieldOrPropertyWithValue("name", "schema.table1");

    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:oracle:thin:fred/sec%40ret@//hostname", "schema.table1", new Properties()))
        .hasFieldOrPropertyWithValue("namespace", "oracle://hostname:1521")
        .hasFieldOrPropertyWithValue("name", "schema.table1");

    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:oracle:thin:fred/sec%40ret@hostname", "schema.table1", new Properties()))
        .hasFieldOrPropertyWithValue("namespace", "oracle://hostname:1521")
        .hasFieldOrPropertyWithValue("name", "schema.table1");
  }

  @Test
  void testGetDatasetIdentifierWithCustomPort() {
    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:oracle:thin:@//hostname:1522", "schema.table1", new Properties()))
        .hasFieldOrPropertyWithValue("namespace", "oracle://hostname:1522")
        .hasFieldOrPropertyWithValue("name", "schema.table1");

    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:oracle:thin:@hostname:1522", "schema.table1", new Properties()))
        .hasFieldOrPropertyWithValue("namespace", "oracle://hostname:1522")
        .hasFieldOrPropertyWithValue("name", "schema.table1");
  }

  @Test
  void testGetDatasetIdentifierWithProtocol() {
    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:oracle:thin:@tcp://hostname", "schema.table1", new Properties()))
        .hasFieldOrPropertyWithValue("namespace", "oracle://hostname:1521")
        .hasFieldOrPropertyWithValue("name", "schema.table1");
  }

  @Test
  void testGetDatasetIdentifierWithServiceName() {
    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:oracle:thin:@//hostname/serviceName", "schema.table1", new Properties()))
        .hasFieldOrPropertyWithValue("namespace", "oracle://hostname:1521")
        .hasFieldOrPropertyWithValue("name", "serviceName.schema.table1");

    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:oracle:thin:@//hostname:1522/serviceName", "schema.table1", new Properties()))
        .hasFieldOrPropertyWithValue("namespace", "oracle://hostname:1522")
        .hasFieldOrPropertyWithValue("name", "serviceName.schema.table1");
  }

  @Test
  void testGetDatasetIdentifierWithSid() {
    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:oracle:thin:@hostname:sid", "schema.table1", new Properties()))
        .hasFieldOrPropertyWithValue("namespace", "oracle://hostname:1521")
        .hasFieldOrPropertyWithValue("name", "sid.schema.table1");

    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:oracle:thin:@hostname:1522:sid", "schema.table1", new Properties()))
        .hasFieldOrPropertyWithValue("namespace", "oracle://hostname:1522")
        .hasFieldOrPropertyWithValue("name", "sid.schema.table1");
  }

  @Test
  void testGetDatasetIdentifierWithExtraProperties() {
    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:oracle:thin:@//hostname?connect_timeout=30&retry_count=3",
                "schema.table1",
                new Properties()))
        .hasFieldOrPropertyWithValue("namespace", "oracle://hostname:1521")
        .hasFieldOrPropertyWithValue("name", "schema.table1");

    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:oracle:thin:@hostname?connect_timeout=30&retry_count=3",
                "schema.table1",
                new Properties()))
        .hasFieldOrPropertyWithValue("namespace", "oracle://hostname:1521")
        .hasFieldOrPropertyWithValue("name", "schema.table1");
  }

  @Test
  void testGetDatasetIdentifierWithUppercaseURL() {
    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "JDBC:ORACLE:THIN:@//TEST.HOST.COM/SERVICENAME", "SCHEMA.TABLE1", new Properties()))
        .hasFieldOrPropertyWithValue("namespace", "oracle://test.host.com:1521")
        .hasFieldOrPropertyWithValue("name", "SERVICENAME.SCHEMA.TABLE1");

    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "JDBC:ORACLE:THIN:@TEST.HOST.COM:SID", "SCHEMA.TABLE1", new Properties()))
        .hasFieldOrPropertyWithValue("namespace", "oracle://test.host.com:1521")
        .hasFieldOrPropertyWithValue("name", "SID.SCHEMA.TABLE1");
  }

  @Test
  void testGetDatasetIdentifierWithMultipleHostsInSimpleFormat() {
    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:oracle:thin:@//myhost1,myhost2", "schema.table1", new Properties()))
        .hasFieldOrPropertyWithValue("namespace", "oracle://myhost1:1521,myhost2:1521")
        .hasFieldOrPropertyWithValue("name", "schema.table1");

    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:oracle:thin:@myhost1:1521,myhost2:1521", "schema.table1", new Properties()))
        .hasFieldOrPropertyWithValue("namespace", "oracle://myhost1:1521,myhost2:1521")
        .hasFieldOrPropertyWithValue("name", "schema.table1");
  }

  @Test
  void testGetDatasetIdentifierWithTnsFormat() {
    // Currently TNS format is not parsed properly. Just drop credentials
    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:oracle:thin:@(DESCRIPTION=(ADDRESS=(PROTOCOL=tcp)(HOST=hostname)(PORT=1521)))(CONNECT_DATA=(INSTANCE_NAME=ORCL)))",
                "schema.table1",
                new Properties()))
        .hasFieldOrPropertyWithValue(
            "namespace",
            "oracle:thin:@(DESCRIPTION=(ADDRESS=(PROTOCOL=tcp)(HOST=hostname)(PORT=1521)))(CONNECT_DATA=(INSTANCE_NAME=ORCL)))")
        .hasFieldOrPropertyWithValue("name", "schema.table1");

    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:oracle:oci@(DESCRIPTION=(ADDRESS=(PROTOCOL=tcp)(HOST=hostname)(PORT=1521)))(CONNECT_DATA=(INSTANCE_NAME=ORCL)))",
                "schema.table1",
                new Properties()))
        .hasFieldOrPropertyWithValue(
            "namespace",
            "oracle:oci@(DESCRIPTION=(ADDRESS=(PROTOCOL=tcp)(HOST=hostname)(PORT=1521)))(CONNECT_DATA=(INSTANCE_NAME=ORCL)))")
        .hasFieldOrPropertyWithValue("name", "schema.table1");
  }

  @Test
  void testGetDatasetIdentifierWithMultipleHostsInTnsFormat() {
    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:oracle:thin:@(DESCRIPTION= (ADDRESS_LIST= (LOAD_BALANCE=ON) (ADDRESS=(PROTOCOL=tcp)(HOST=salesserver1)(PORT=1521)) (ADDRESS=(PROTOCOL=tcp)(HOST=salesserver2)(PORT=1522))(ADDRESS=(PROTOCOL=tcp)(HOST=salesserver3)(PORT=1522)))(CONNECT_DATA=(SERVICE_NAME=sales.us.example.com)))",
                "schema.table1",
                new Properties()))
        .hasFieldOrPropertyWithValue(
            "namespace",
            "oracle:thin:@(DESCRIPTION= (ADDRESS_LIST= (LOAD_BALANCE=ON) (ADDRESS=(PROTOCOL=tcp)(HOST=salesserver1)(PORT=1521)) (ADDRESS=(PROTOCOL=tcp)(HOST=salesserver2)(PORT=1522))(ADDRESS=(PROTOCOL=tcp)(HOST=salesserver3)(PORT=1522)))(CONNECT_DATA=(SERVICE_NAME=sales.us.example.com)))")
        .hasFieldOrPropertyWithValue("name", "schema.table1");

    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:oracle:oci@(DESCRIPTION= (ADDRESS_LIST= (LOAD_BALANCE=ON) (ADDRESS=(PROTOCOL=tcp)(HOST=salesserver1)(PORT=1521)) (ADDRESS=(PROTOCOL=tcp)(HOST=salesserver2)(PORT=1522))(ADDRESS=(PROTOCOL=tcp)(HOST=salesserver3)(PORT=1522)))(CONNECT_DATA=(SERVICE_NAME=sales.us.example.com)))",
                "schema.table1",
                new Properties()))
        .hasFieldOrPropertyWithValue(
            "namespace",
            "oracle:oci@(DESCRIPTION= (ADDRESS_LIST= (LOAD_BALANCE=ON) (ADDRESS=(PROTOCOL=tcp)(HOST=salesserver1)(PORT=1521)) (ADDRESS=(PROTOCOL=tcp)(HOST=salesserver2)(PORT=1522))(ADDRESS=(PROTOCOL=tcp)(HOST=salesserver3)(PORT=1522)))(CONNECT_DATA=(SERVICE_NAME=sales.us.example.com)))")
        .hasFieldOrPropertyWithValue("name", "schema.table1");
  }

  @Test
  void testGetDatasetIdentifierWithLDAPFormat() {
    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:oracle:thin:@ldap://oid:5000/mydb1,cn=OracleContext,dc=myco,dc=com",
                "schema.table1",
                new Properties()))
        .hasFieldOrPropertyWithValue("namespace", "oracle://oid:5000")
        .hasFieldOrPropertyWithValue("name", "mydb1.schema.table1");

    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:oracle:thin:@ldap:ldap.example.com:5000/mydb1,cn=OracleContext,dc=com",
                "schema.table1",
                new Properties()))
        .hasFieldOrPropertyWithValue("namespace", "oracle://ldap.example.com:5000")
        .hasFieldOrPropertyWithValue("name", "mydb1.schema.table1");

    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:oracle:thin:@ldaps:ldaps.example.com:5000/mydb1,cn=OracleContext,dc=com",
                "schema.table1",
                new Properties()))
        .hasFieldOrPropertyWithValue("namespace", "oracle://ldaps.example.com:5000")
        .hasFieldOrPropertyWithValue("name", "mydb1.schema.table1");

    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:oracle:thin:@ldap://ldap1.example.com:5000/cn=salesdept,cn=OracleContext,dc=com/mydb1 ldap://ldap2.example.com:3500/cn=salesdept,cn=OracleContext,dc=com/mydb1 ldap://ldap3.example.com:3500/cn=salesdept,cn=OracleContext,dc=com/mydb1",
                "schema.table1",
                new Properties()))
        .hasFieldOrPropertyWithValue("namespace", "oracle://ldap1.example.com:5000")
        .hasFieldOrPropertyWithValue("name", "mydb1.schema.table1");

    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:oracle:thin:@ldap://ldap1.example.com:5000/cn=salesdept,cn=OracleContext,dc=com/mydb1",
                "schema.table1",
                new Properties()))
        .hasFieldOrPropertyWithValue("namespace", "oracle://ldap1.example.com:5000")
        .hasFieldOrPropertyWithValue("name", "mydb1.schema.table1");

    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:oracle:thin:@ldap:ldap1.example.com:5000/cn=salesdept,cn=OracleContext,dc=com/mydb1",
                "schema.table1",
                new Properties()))
        .hasFieldOrPropertyWithValue("namespace", "oracle://ldap1.example.com:5000")
        .hasFieldOrPropertyWithValue("name", "mydb1.schema.table1");

    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:oracle:thin:@ldap:ldap1.example.com:5000/cn=salesdept,cn=OracleContext,dc=com/mydb1 ldap:ldap2.example.com:3500/cn=salesdept,cn=OracleContext,dc=com/mydb1 ldap:ldap3.example.com:3500/cn=salesdept,cn=OracleContext,dc=com/mydb1",
                "schema.table1",
                new Properties()))
        .hasFieldOrPropertyWithValue("namespace", "oracle://ldap1.example.com:5000")
        .hasFieldOrPropertyWithValue("name", "mydb1.schema.table1");
  }
}
