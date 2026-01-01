/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.utils.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Properties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

@SuppressWarnings("PMD.AvoidUsingHardCodedIP")
class JdbcDatasetUtilsTestForDerby {
  private static final String CURRENT_DIR_PROPERTY = "user.dir";
  private static final String DERBY_HOME_PROPERTY = "derby.system.home";

  @AfterEach
  void restoreDerbyHome() {
    System.clearProperty(DERBY_HOME_PROPERTY);
  }

  @Test
  void testGetDatasetIdentifierWithDefaultDatabase() {
    String currentDir = System.getProperty(CURRENT_DIR_PROPERTY);
    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier("jdbc:derby:", "schema.table1", new Properties()))
        .hasFieldOrPropertyWithValue("namespace", "file:" + currentDir + "/metastore_db")
        .hasFieldOrPropertyWithValue("name", "schema.table1");
  }

  @Test
  void testGetDatasetIdentifierWithCustomDatabaseName() {
    String currentDir = System.getProperty(CURRENT_DIR_PROPERTY);
    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:derby:;databaseName=somedb", "schema.table1", new Properties()))
        .hasFieldOrPropertyWithValue("namespace", "file:" + currentDir + "/somedb")
        .hasFieldOrPropertyWithValue("name", "schema.table1");
  }

  @Test
  void testGetDatasetIdentifierWithCustomDerbyHome() {
    System.setProperty(DERBY_HOME_PROPERTY, "/some/path");

    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier("jdbc:derby:", "schema.table1", new Properties()))
        .hasFieldOrPropertyWithValue("namespace", "file:/some/path/metastore_db")
        .hasFieldOrPropertyWithValue("name", "schema.table1");
  }

  @Test
  void testGetDatasetIdentifierWithCustomDatabasePath() {
    System.setProperty(DERBY_HOME_PROPERTY, "/some/path");

    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:derby:;databaseName=nested/path", "schema.table1", new Properties()))
        .hasFieldOrPropertyWithValue("namespace", "file:/some/path/nested/path")
        .hasFieldOrPropertyWithValue("name", "schema.table1");

    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:derby:;databaseName=../parent/path", "schema.table1", new Properties()))
        .hasFieldOrPropertyWithValue("namespace", "file:/some/parent/path")
        .hasFieldOrPropertyWithValue("name", "schema.table1");
  }
}
