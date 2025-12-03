/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.catalog;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.client.utils.gravitino.GravitinoInfoProviderImpl;
import io.openlineage.spark.api.OpenLineageContext;
import java.util.HashMap;
import lombok.SneakyThrows;
import org.apache.gravitino.spark.connector.catalog.BaseCatalog;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@SuppressWarnings("PMD.AvoidDuplicateLiterals")
class GravitinoHandlerTest {

  private OpenLineageContext context;
  private GravitinoHandler gravitinoHandler;
  private SparkSession sparkSession;
  private BaseCatalog gravitinoCatalog;

  @BeforeEach
  void setUp() {
    context = mock(OpenLineageContext.class);
    gravitinoHandler = new GravitinoHandler(context);
    sparkSession = mock(SparkSession.class);
    gravitinoCatalog = mock(BaseCatalog.class);
  }

  @AfterEach
  void tearDown() {
    // Clean up Spark session if needed
    try {
      if (sparkSession != null) {
        sparkSession.stop();
      }
    } catch (Exception e) {
      // Ignore cleanup errors
    }
  }

  @Test
  void testHasClasses() {
    // Test that Gravitino classes are detected
    boolean hasClasses = gravitinoHandler.hasClasses();
    // This will be true if gravitino-spark-connector is in classpath
    assertThat(hasClasses).isIn(true, false);
  }

  @Test
  void testIsClass() {
    // Test that BaseCatalog is recognized
    boolean isGravitinoCatalog = gravitinoHandler.isClass(gravitinoCatalog);
    assertThat(isGravitinoCatalog).isTrue();
  }

  @Test
  void testGetName() {
    assertThat(gravitinoHandler.getName()).isEqualTo("gravitino");
  }

  @Test
  @SneakyThrows
  void testGetDatasetIdentifierWithMetalake() {
    // Setup Spark session with Gravitino configuration
    SparkSession testSession =
        SparkSession.builder()
            .master("local[*]")
            .appName("GravitinoHandlerTest")
            .config("spark.sql.gravitino.metalake", "test_metalake")
            .getOrCreate();

    try {
      when(gravitinoCatalog.name()).thenReturn("test_catalog");
      when(gravitinoCatalog.defaultNamespace()).thenReturn(new String[] {"test_schema"});

      Identifier identifier = Identifier.of(new String[] {"test_schema"}, "test_table");

      DatasetIdentifier datasetIdentifier =
          gravitinoHandler.getDatasetIdentifier(
              testSession, gravitinoCatalog, identifier, new HashMap<>());

      assertThat(datasetIdentifier)
          .hasFieldOrPropertyWithValue("namespace", "test_metalake")
          .hasFieldOrPropertyWithValue("name", "test_catalog.test_schema.test_table");
    } finally {
      testSession.stop();
      org.apache.spark.sql.SparkSession$.MODULE$.cleanupAnyExistingSession();
    }
  }

  @Test
  @SneakyThrows
  void testGetDatasetIdentifierWithNestedNamespace() {
    SparkSession testSession =
        SparkSession.builder()
            .master("local[*]")
            .appName("GravitinoHandlerTest")
            .config("spark.sql.gravitino.metalake", "prod_metalake")
            .getOrCreate();

    try {
      when(gravitinoCatalog.name()).thenReturn("iceberg_catalog");
      when(gravitinoCatalog.defaultNamespace())
          .thenReturn(new String[] {"level1", "level2", "level3"});

      Identifier identifier =
          Identifier.of(new String[] {"level1", "level2", "level3"}, "nested_table");

      DatasetIdentifier datasetIdentifier =
          gravitinoHandler.getDatasetIdentifier(
              testSession, gravitinoCatalog, identifier, new HashMap<>());

      assertThat(datasetIdentifier)
          .hasFieldOrPropertyWithValue("namespace", "prod_metalake")
          .hasFieldOrPropertyWithValue("name", "iceberg_catalog.level1.level2.level3.nested_table");
    } finally {
      testSession.stop();
      org.apache.spark.sql.SparkSession$.MODULE$.cleanupAnyExistingSession();
    }
  }

  @Test
  void testGetDatasetIdentifierWithoutMetalakeThrowsException() {
    SparkSession testSession =
        SparkSession.builder()
            .master("local[*]")
            .appName("GravitinoHandlerTest")
            // No metalake configuration
            .getOrCreate();

    try {
      when(gravitinoCatalog.name()).thenReturn("test_catalog");
      when(gravitinoCatalog.defaultNamespace()).thenReturn(new String[] {"test_schema"});

      Identifier identifier = Identifier.of(new String[] {"test_schema"}, "test_table");

      // Create a new provider instance for this test
      GravitinoInfoProviderImpl testProvider = GravitinoInfoProviderImpl.newInstanceForTest();

      IllegalStateException exception =
          assertThrows(
              IllegalStateException.class,
              () ->
                  gravitinoHandler.getDatasetIdentifier(
                      testSession, gravitinoCatalog, identifier, new HashMap<>()));

      assertThat(exception.getMessage())
          .contains("Gravitino metalake configuration not found")
          .contains("spark.sql.gravitino.metalake")
          .contains("spark.hadoop.fs.gravitino.client.metalake");
    } finally {
      testSession.stop();
      org.apache.spark.sql.SparkSession$.MODULE$.cleanupAnyExistingSession();
    }
  }

  @Test
  @SneakyThrows
  void testGetDatasetIdentifierWithEmptyNamespace() {
    SparkSession testSession =
        SparkSession.builder()
            .master("local[*]")
            .appName("GravitinoHandlerTest")
            .config("spark.sql.gravitino.metalake", "test_metalake")
            .getOrCreate();

    try {
      when(gravitinoCatalog.name()).thenReturn("test_catalog");
      when(gravitinoCatalog.defaultNamespace()).thenReturn(new String[] {"default"});

      // Identifier with empty namespace - should use default
      Identifier identifier = Identifier.of(new String[] {}, "table_no_namespace");

      DatasetIdentifier datasetIdentifier =
          gravitinoHandler.getDatasetIdentifier(
              testSession, gravitinoCatalog, identifier, new HashMap<>());

      assertThat(datasetIdentifier)
          .hasFieldOrPropertyWithValue("namespace", "test_metalake")
          .hasFieldOrPropertyWithValue("name", "test_catalog.default.table_no_namespace");
    } finally {
      testSession.stop();
      org.apache.spark.sql.SparkSession$.MODULE$.cleanupAnyExistingSession();
    }
  }

  @Test
  @SneakyThrows
  void testGetDatasetIdentifierWithFilesystemMetalake() {
    // Test using filesystem metalake configuration
    SparkSession testSession =
        SparkSession.builder()
            .master("local[*]")
            .appName("GravitinoHandlerTest")
            .config("spark.hadoop.fs.gravitino.client.metalake", "fs_metalake")
            .getOrCreate();

    try {
      when(gravitinoCatalog.name()).thenReturn("test_catalog");
      when(gravitinoCatalog.defaultNamespace()).thenReturn(new String[] {"test_schema"});

      Identifier identifier = Identifier.of(new String[] {"test_schema"}, "test_table");

      DatasetIdentifier datasetIdentifier =
          gravitinoHandler.getDatasetIdentifier(
              testSession, gravitinoCatalog, identifier, new HashMap<>());

      assertThat(datasetIdentifier)
          .hasFieldOrPropertyWithValue("namespace", "fs_metalake")
          .hasFieldOrPropertyWithValue("name", "test_catalog.test_schema.test_table");
    } finally {
      testSession.stop();
      org.apache.spark.sql.SparkSession$.MODULE$.cleanupAnyExistingSession();
    }
  }

  @Test
  @SneakyThrows
  void testGetDatasetIdentifierConnectorMetalakeTakesPrecedence() {
    // Test that connector metalake takes precedence over filesystem metalake
    SparkSession testSession =
        SparkSession.builder()
            .master("local[*]")
            .appName("GravitinoHandlerTest")
            .config("spark.sql.gravitino.metalake", "connector_metalake")
            .config("spark.hadoop.fs.gravitino.client.metalake", "fs_metalake")
            .getOrCreate();

    try {
      when(gravitinoCatalog.name()).thenReturn("test_catalog");
      when(gravitinoCatalog.defaultNamespace()).thenReturn(new String[] {"test_schema"});

      Identifier identifier = Identifier.of(new String[] {"test_schema"}, "test_table");

      DatasetIdentifier datasetIdentifier =
          gravitinoHandler.getDatasetIdentifier(
              testSession, gravitinoCatalog, identifier, new HashMap<>());

      // Should use connector_metalake, not fs_metalake
      assertThat(datasetIdentifier)
          .hasFieldOrPropertyWithValue("namespace", "connector_metalake")
          .hasFieldOrPropertyWithValue("name", "test_catalog.test_schema.test_table");
    } finally {
      testSession.stop();
      org.apache.spark.sql.SparkSession$.MODULE$.cleanupAnyExistingSession();
    }
  }
}
