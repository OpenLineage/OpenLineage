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
class GravitinoIcebergHandlerTest {

  private OpenLineageContext context;
  private GravitinoIcebergHandler gravitinoIcebergHandler;
  private SparkSession sparkSession;
  private TableCatalog icebergCatalog;

  @BeforeEach
  void setUp() {
    // Clear the cached Gravitino configuration before each test
    GravitinoInfoProviderImpl.getInstance().clearCache();
    context = mock(OpenLineageContext.class);
    gravitinoIcebergHandler = new GravitinoIcebergHandler(context);
    sparkSession = mock(SparkSession.class);
    icebergCatalog = mock(TableCatalog.class);
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
  void testGetDatasetIdentifierWithoutCatalogMapping() {
    SparkSession testSession =
        SparkSession.builder()
            .master("local[*]")
            .appName("GravitinoIcebergHandlerTest")
            .config("spark.sql.gravitino.metalake", "test_metalake")
            .getOrCreate();

    try {
      when(icebergCatalog.name()).thenReturn("iceberg_catalog");
      when(icebergCatalog.defaultNamespace()).thenReturn(new String[] {"db"});

      Identifier identifier = Identifier.of(new String[] {"db"}, "table");

      DatasetIdentifier datasetIdentifier =
          gravitinoIcebergHandler.getDatasetIdentifier(
              testSession, icebergCatalog, identifier, new HashMap<>());

      // Without catalog mapping, catalog name should be used as-is
      assertThat(datasetIdentifier)
          .hasFieldOrPropertyWithValue("namespace", "test_metalake")
          .hasFieldOrPropertyWithValue("name", "iceberg_catalog.db.table");
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
            .appName("GravitinoIcebergHandlerTest")
            .config("spark.sql.gravitino.metalake", "prod_metalake")
            .config(
                "spark.sql.gravitino.catalogMappings",
                "local_iceberg:prod_iceberg,local_jdbc:prod_postgres")
            .getOrCreate();

    try {
      when(icebergCatalog.name()).thenReturn("local_iceberg");
      when(icebergCatalog.defaultNamespace()).thenReturn(new String[] {"sales"});

      Identifier identifier = Identifier.of(new String[] {"sales"}, "orders");

      DatasetIdentifier datasetIdentifier =
          gravitinoIcebergHandler.getDatasetIdentifier(
              testSession, icebergCatalog, identifier, new HashMap<>());

      // Catalog name should be mapped from local_iceberg to prod_iceberg
      assertThat(datasetIdentifier)
          .hasFieldOrPropertyWithValue("namespace", "prod_metalake")
          .hasFieldOrPropertyWithValue("name", "prod_iceberg.sales.orders");
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
            .appName("GravitinoIcebergHandlerTest")
            .config("spark.sql.gravitino.metalake", "test_metalake")
            .config("spark.sql.gravitino.catalogMappings", "catalog1:gravitino1")
            .getOrCreate();

    try {
      // Using a catalog that's not in the mapping
      when(icebergCatalog.name()).thenReturn("unmapped_catalog");
      when(icebergCatalog.defaultNamespace()).thenReturn(new String[] {"db"});

      Identifier identifier = Identifier.of(new String[] {"db"}, "table");

      DatasetIdentifier datasetIdentifier =
          gravitinoIcebergHandler.getDatasetIdentifier(
              testSession, icebergCatalog, identifier, new HashMap<>());

      // Unmapped catalog should use original name
      assertThat(datasetIdentifier)
          .hasFieldOrPropertyWithValue("namespace", "test_metalake")
          .hasFieldOrPropertyWithValue("name", "unmapped_catalog.db.table");
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
            .appName("GravitinoIcebergHandlerTest")
            .config("spark.sql.gravitino.metalake", "test_metalake")
            .getOrCreate();

    try {
      when(icebergCatalog.name()).thenReturn("iceberg_catalog");
      when(icebergCatalog.defaultNamespace()).thenReturn(new String[] {"level1", "level2"});

      Identifier identifier = Identifier.of(new String[] {"level1", "level2", "level3"}, "table");

      DatasetIdentifier datasetIdentifier =
          gravitinoIcebergHandler.getDatasetIdentifier(
              testSession, icebergCatalog, identifier, new HashMap<>());

      assertThat(datasetIdentifier)
          .hasFieldOrPropertyWithValue("namespace", "test_metalake")
          .hasFieldOrPropertyWithValue("name", "iceberg_catalog.level1.level2.level3.table");
    } finally {
      testSession.stop();
      org.apache.spark.sql.SparkSession$.MODULE$.cleanupAnyExistingSession();
    }
  }

  @Test
  @SneakyThrows
  void testGetDatasetIdentifierWithEmptyIdentifierNamespace() {
    SparkSession testSession =
        SparkSession.builder()
            .master("local[*]")
            .appName("GravitinoIcebergHandlerTest")
            .config("spark.sql.gravitino.metalake", "test_metalake")
            .getOrCreate();

    try {
      when(icebergCatalog.name()).thenReturn("iceberg_catalog");
      when(icebergCatalog.defaultNamespace()).thenReturn(new String[] {"default_db"});

      // Identifier with empty namespace - should use catalog's default namespace
      Identifier identifier = Identifier.of(new String[] {}, "table");

      DatasetIdentifier datasetIdentifier =
          gravitinoIcebergHandler.getDatasetIdentifier(
              testSession, icebergCatalog, identifier, new HashMap<>());

      assertThat(datasetIdentifier)
          .hasFieldOrPropertyWithValue("namespace", "test_metalake")
          .hasFieldOrPropertyWithValue("name", "iceberg_catalog.default_db.table");
    } finally {
      testSession.stop();
      org.apache.spark.sql.SparkSession$.MODULE$.cleanupAnyExistingSession();
    }
  }

  @Test
  @SneakyThrows
  void testGetDatasetIdentifierWithMultipleCatalogMappings() {
    SparkSession testSession =
        SparkSession.builder()
            .master("local[*]")
            .appName("GravitinoIcebergHandlerTest")
            .config("spark.sql.gravitino.metalake", "prod_metalake")
            .config(
                "spark.sql.gravitino.catalogMappings",
                "dev_iceberg:prod_iceberg,dev_hive:prod_hive,dev_jdbc:prod_postgres")
            .getOrCreate();

    try {
      // Test first mapping
      when(icebergCatalog.name()).thenReturn("dev_iceberg");
      when(icebergCatalog.defaultNamespace()).thenReturn(new String[] {"db1"});

      Identifier identifier1 = Identifier.of(new String[] {"db1"}, "table1");

      DatasetIdentifier datasetIdentifier1 =
          gravitinoIcebergHandler.getDatasetIdentifier(
              testSession, icebergCatalog, identifier1, new HashMap<>());

      assertThat(datasetIdentifier1)
          .hasFieldOrPropertyWithValue("namespace", "prod_metalake")
          .hasFieldOrPropertyWithValue("name", "prod_iceberg.db1.table1");

      // Test second mapping
      when(icebergCatalog.name()).thenReturn("dev_hive");
      when(icebergCatalog.defaultNamespace()).thenReturn(new String[] {"db2"});

      Identifier identifier2 = Identifier.of(new String[] {"db2"}, "table2");

      DatasetIdentifier datasetIdentifier2 =
          gravitinoIcebergHandler.getDatasetIdentifier(
              testSession, icebergCatalog, identifier2, new HashMap<>());

      assertThat(datasetIdentifier2)
          .hasFieldOrPropertyWithValue("namespace", "prod_metalake")
          .hasFieldOrPropertyWithValue("name", "prod_hive.db2.table2");
    } finally {
      testSession.stop();
      org.apache.spark.sql.SparkSession$.MODULE$.cleanupAnyExistingSession();
    }
  }
}
