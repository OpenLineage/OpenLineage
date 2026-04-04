/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.utils.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
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
    String currentDir =
        new File(System.getProperty(CURRENT_DIR_PROPERTY)).toURI().normalize().getPath();
    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier("jdbc:derby:", "schema.table1", new Properties()))
        .hasFieldOrPropertyWithValue("namespace", "file:" + currentDir + "metastore_db")
        .hasFieldOrPropertyWithValue("name", "schema.table1");
  }

  @Test
  void testGetDatasetIdentifierWithCustomDatabaseName() {
    String currentDir =
        new File(System.getProperty(CURRENT_DIR_PROPERTY)).toURI().normalize().getPath();
    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:derby:;databaseName=somedb", "schema.table1", new Properties()))
        .hasFieldOrPropertyWithValue("namespace", "file:" + currentDir + "somedb")
        .hasFieldOrPropertyWithValue("name", "schema.table1");
  }

  @Test
  void testGetDatasetIdentifierWithCustomDerbyHome() {
    System.setProperty(DERBY_HOME_PROPERTY, "/some/path");

    // On Windows, /some/path becomes C:/some/path, on Unix it stays /some/path
    String expectedPath = new File("/some/path/metastore_db").toURI().normalize().getPath();

    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier("jdbc:derby:", "schema.table1", new Properties()))
        .hasFieldOrPropertyWithValue("namespace", "file:" + expectedPath)
        .hasFieldOrPropertyWithValue("name", "schema.table1");
  }

  @Test
  void testGetDatasetIdentifierWithCustomDatabasePath() {
    System.setProperty(DERBY_HOME_PROPERTY, "/some/path");

    // On Windows, /some/path becomes C:/some/path, on Unix it stays /some/path
    String expectedPath1 = new File("/some/path/nested/path").toURI().normalize().getPath();
    String expectedPath2 = new File("/some/parent/path").toURI().normalize().getPath();

    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:derby:;databaseName=nested/path", "schema.table1", new Properties()))
        .hasFieldOrPropertyWithValue("namespace", "file:" + expectedPath1)
        .hasFieldOrPropertyWithValue("name", "schema.table1");

    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:derby:;databaseName=../parent/path", "schema.table1", new Properties()))
        .hasFieldOrPropertyWithValue("namespace", "file:" + expectedPath2)
        .hasFieldOrPropertyWithValue("name", "schema.table1");
  }
}
