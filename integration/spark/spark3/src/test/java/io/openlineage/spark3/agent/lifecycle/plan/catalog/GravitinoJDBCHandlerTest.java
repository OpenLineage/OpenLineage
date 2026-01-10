/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.catalog;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.client.utils.gravitino.GravitinoInfoProviderImpl;
import io.openlineage.spark.api.OpenLineageContext;
import java.util.HashMap;
import lombok.SneakyThrows;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@SuppressWarnings("PMD.AvoidDuplicateLiterals")
class GravitinoJDBCHandlerTest {

  private OpenLineageContext context;
  private GravitinoJDBCHandler gravitinoJDBCHandler;
  private SparkSession sparkSession;
  private TableCatalog jdbcCatalog;

  @BeforeEach
  void setUp() {
    // Clear the cached Gravitino configuration before each test
    GravitinoInfoProviderImpl.getInstance().clearCache();
    context = mock(OpenLineageContext.class);
    gravitinoJDBCHandler = new GravitinoJDBCHandler(context);
    sparkSession = mock(SparkSession.class);
    jdbcCatalog = mock(TableCatalog.class);
  }

  @AfterEach
  void tearDown() {
    try {
      if (sparkSession != null) {
        sparkSession.stop();
      }
    } catch (Exception e) {
      // Ignore cleanup errors
    }
  }

  @Test
  @SneakyThrows
  void testGetDatasetIdentifierForPostgres() {
    SparkSession testSession =
        SparkSession.builder()
            .master("local[*]")
            .appName("GravitinoJDBCHandlerTest")
            .config("spark.sql.gravitino.metalake", "test_metalake")
            .getOrCreate();

    try {
      when(jdbcCatalog.name()).thenReturn("postgres_catalog");
      when(jdbcCatalog.defaultNamespace()).thenReturn(new String[] {"public"});

      Identifier identifier = Identifier.of(new String[] {"public"}, "users");

      DatasetIdentifier datasetIdentifier =
          gravitinoJDBCHandler.getDatasetIdentifier(
              testSession, jdbcCatalog, identifier, new HashMap<>());

      assertThat(datasetIdentifier)
          .hasFieldOrPropertyWithValue("namespace", "test_metalake")
          .hasFieldOrPropertyWithValue("name", "postgres_catalog.public.users");
    } finally {
      testSession.stop();
      org.apache.spark.sql.SparkSession$.MODULE$.cleanupAnyExistingSession();
    }
  }

  @Test
  @SneakyThrows
  void testGetDatasetIdentifierWithCatalogMapping() {
    SparkSession testSession =
        SparkSession.builder()
            .master("local[*]")
            .appName("GravitinoJDBCHandlerTest")
            .config("spark.sql.gravitino.metalake", "prod_metalake")
            .config(
                "spark.sql.gravitino.catalogMappings",
                "local_postgres:prod_postgres,local_mysql:prod_mysql")
            .getOrCreate();

    try {
      when(jdbcCatalog.name()).thenReturn("local_postgres");
      when(jdbcCatalog.defaultNamespace()).thenReturn(new String[] {"analytics"});

      Identifier identifier = Identifier.of(new String[] {"analytics"}, "metrics");

      DatasetIdentifier datasetIdentifier =
          gravitinoJDBCHandler.getDatasetIdentifier(
              testSession, jdbcCatalog, identifier, new HashMap<>());

      // Catalog name should be mapped from local_postgres to prod_postgres
      assertThat(datasetIdentifier)
          .hasFieldOrPropertyWithValue("namespace", "prod_metalake")
          .hasFieldOrPropertyWithValue("name", "prod_postgres.analytics.metrics");
    } finally {
      testSession.stop();
      org.apache.spark.sql.SparkSession$.MODULE$.cleanupAnyExistingSession();
    }
  }

  @Test
  @SneakyThrows
  void testGetDatasetIdentifierForMySQL() {
    SparkSession testSession =
        SparkSession.builder()
            .master("local[*]")
            .appName("GravitinoJDBCHandlerTest")
            .config("spark.sql.gravitino.metalake", "test_metalake")
            .config("spark.sql.gravitino.catalogMappings", "local_mysql:gravitino_mysql")
            .getOrCreate();

    try {
      when(jdbcCatalog.name()).thenReturn("local_mysql");
      when(jdbcCatalog.defaultNamespace()).thenReturn(new String[] {"mydb"});

      Identifier identifier = Identifier.of(new String[] {"mydb"}, "orders");

      DatasetIdentifier datasetIdentifier =
          gravitinoJDBCHandler.getDatasetIdentifier(
              testSession, jdbcCatalog, identifier, new HashMap<>());

      assertThat(datasetIdentifier)
          .hasFieldOrPropertyWithValue("namespace", "test_metalake")
          .hasFieldOrPropertyWithValue("name", "gravitino_mysql.mydb.orders");
    } finally {
      testSession.stop();
      org.apache.spark.sql.SparkSession$.MODULE$.cleanupAnyExistingSession();
    }
  }

  @Test
  @SneakyThrows
  void testGetDatasetIdentifierWithMultipleSchemas() {
    SparkSession testSession =
        SparkSession.builder()
            .master("local[*]")
            .appName("GravitinoJDBCHandlerTest")
            .config("spark.sql.gravitino.metalake", "test_metalake")
            .getOrCreate();

    try {
      when(jdbcCatalog.name()).thenReturn("jdbc_catalog");
      when(jdbcCatalog.defaultNamespace()).thenReturn(new String[] {"schema1"});

      // Test with schema1
      Identifier identifier1 = Identifier.of(new String[] {"schema1"}, "table1");
      DatasetIdentifier datasetIdentifier1 =
          gravitinoJDBCHandler.getDatasetIdentifier(
              testSession, jdbcCatalog, identifier1, new HashMap<>());

      assertThat(datasetIdentifier1)
          .hasFieldOrPropertyWithValue("namespace", "test_metalake")
          .hasFieldOrPropertyWithValue("name", "jdbc_catalog.schema1.table1");

      // Test with schema2
      Identifier identifier2 = Identifier.of(new String[] {"schema2"}, "table2");
      DatasetIdentifier datasetIdentifier2 =
          gravitinoJDBCHandler.getDatasetIdentifier(
              testSession, jdbcCatalog, identifier2, new HashMap<>());

      assertThat(datasetIdentifier2)
          .hasFieldOrPropertyWithValue("namespace", "test_metalake")
          .hasFieldOrPropertyWithValue("name", "jdbc_catalog.schema2.table2");
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
            .appName("GravitinoJDBCHandlerTest")
            .config("spark.sql.gravitino.metalake", "test_metalake")
            .getOrCreate();

    try {
      when(jdbcCatalog.name()).thenReturn("jdbc_catalog");
      when(jdbcCatalog.defaultNamespace()).thenReturn(new String[] {"default_schema"});

      // Identifier with empty namespace - should use catalog's default namespace
      Identifier identifier = Identifier.of(new String[] {}, "table");

      DatasetIdentifier datasetIdentifier =
          gravitinoJDBCHandler.getDatasetIdentifier(
              testSession, jdbcCatalog, identifier, new HashMap<>());

      assertThat(datasetIdentifier)
          .hasFieldOrPropertyWithValue("namespace", "test_metalake")
          .hasFieldOrPropertyWithValue("name", "jdbc_catalog.default_schema.table");
    } finally {
      testSession.stop();
      org.apache.spark.sql.SparkSession$.MODULE$.cleanupAnyExistingSession();
    }
  }

  @Test
  @SneakyThrows
  void testGetDatasetIdentifierWithUnmappedCatalog() {
    SparkSession testSession =
        SparkSession.builder()
            .master("local[*]")
            .appName("GravitinoJDBCHandlerTest")
            .config("spark.sql.gravitino.metalake", "test_metalake")
            .config("spark.sql.gravitino.catalogMappings", "catalog1:gravitino1")
            .getOrCreate();

    try {
      // Using a catalog that's not in the mapping
      when(jdbcCatalog.name()).thenReturn("unmapped_jdbc");
      when(jdbcCatalog.defaultNamespace()).thenReturn(new String[] {"public"});

      Identifier identifier = Identifier.of(new String[] {"public"}, "table");

      DatasetIdentifier datasetIdentifier =
          gravitinoJDBCHandler.getDatasetIdentifier(
              testSession, jdbcCatalog, identifier, new HashMap<>());

      // Unmapped catalog should use original name
      assertThat(datasetIdentifier)
          .hasFieldOrPropertyWithValue("namespace", "test_metalake")
          .hasFieldOrPropertyWithValue("name", "unmapped_jdbc.public.table");
    } finally {
      testSession.stop();
      org.apache.spark.sql.SparkSession$.MODULE$.cleanupAnyExistingSession();
    }
  }

  @Test
  @SneakyThrows
  void testGetDatasetIdentifierWithComplexTableName() {
    SparkSession testSession =
        SparkSession.builder()
            .master("local[*]")
            .appName("GravitinoJDBCHandlerTest")
            .config("spark.sql.gravitino.metalake", "test_metalake")
            .getOrCreate();

    try {
      when(jdbcCatalog.name()).thenReturn("jdbc_catalog");
      when(jdbcCatalog.defaultNamespace()).thenReturn(new String[] {"schema"});

      // Table name with underscores and numbers
      Identifier identifier = Identifier.of(new String[] {"schema"}, "user_events_2024");

      DatasetIdentifier datasetIdentifier =
          gravitinoJDBCHandler.getDatasetIdentifier(
              testSession, jdbcCatalog, identifier, new HashMap<>());

      assertThat(datasetIdentifier)
          .hasFieldOrPropertyWithValue("namespace", "test_metalake")
          .hasFieldOrPropertyWithValue("name", "jdbc_catalog.schema.user_events_2024");
    } finally {
      testSession.stop();
      org.apache.spark.sql.SparkSession$.MODULE$.cleanupAnyExistingSession();
    }
  }
}
