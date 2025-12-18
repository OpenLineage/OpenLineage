/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.util;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.SparkSession$;
import static org.assertj.core.api.Assertions.assertThat;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import io.openlineage.client.utils.gravitino.GravitinoInfoProviderImpl;
import lombok.SneakyThrows;

@SuppressWarnings("PMD.AvoidDuplicateLiterals")
class GravitinoCatalogMappingTest {

  @AfterEach
  void tearDown() {
    SparkSession$.MODULE$.cleanupAnyExistingSession();
    GravitinoInfoProviderImpl.getInstance().clearCache();
  }

  @Test
  @SneakyThrows
  void testValidCatalogMapping() {
    SparkSession testSession =
        SparkSession.builder()
            .master("local[*]")
            .appName("GravitinoCatalogMappingTest")
            .config(SparkGravitinoInfoProvider.metalakeConfigKeyForConnector, "test_metalake")
            .config(SparkGravitinoInfoProvider.useGravitinoConfigKey, "true")
            .config(
                SparkGravitinoInfoProvider.catalogMappingConfigKey,
                "catalog1:gravitino1,catalog2:gravitino2")
            .getOrCreate();

    try {
      GravitinoInfoProviderImpl provider = GravitinoInfoProviderImpl.newInstanceForTest();

      assertThat(provider.getGravitinoCatalog("catalog1")).isEqualTo("gravitino1");
      assertThat(provider.getGravitinoCatalog("catalog2")).isEqualTo("gravitino2");
      assertThat(provider.getGravitinoCatalog("catalog3")).isEqualTo("catalog3");
    } finally {
      testSession.stop();
    }
  }

  @Test
  @SneakyThrows
  void testCatalogMappingWithSpaces() {
    SparkSession testSession =
        SparkSession.builder()
            .master("local[*]")
            .appName("GravitinoCatalogMappingTest")
            .config(SparkGravitinoInfoProvider.metalakeConfigKeyForConnector, "test_metalake")
            .config(SparkGravitinoInfoProvider.useGravitinoConfigKey, "true")
            .config(
                SparkGravitinoInfoProvider.catalogMappingConfigKey,
                "catalog1 : gravitino1 , catalog2 : gravitino2")
            .getOrCreate();

    try {
      GravitinoInfoProviderImpl provider = GravitinoInfoProviderImpl.newInstanceForTest();

      // Spaces should be trimmed
      assertThat(provider.getGravitinoCatalog("catalog1")).isEqualTo("gravitino1");
      assertThat(provider.getGravitinoCatalog("catalog2")).isEqualTo("gravitino2");
    } finally {
      testSession.stop();
    }
  }

  @Test
  @SneakyThrows
  void testInvalidCatalogMappingEmptyKey() {
    SparkSession testSession =
        SparkSession.builder()
            .master("local[*]")
            .appName("GravitinoCatalogMappingTest")
            .config(SparkGravitinoInfoProvider.metalakeConfigKeyForConnector, "test_metalake")
            .config(SparkGravitinoInfoProvider.useGravitinoConfigKey, "true")
            .config(
                SparkGravitinoInfoProvider.catalogMappingConfigKey,
                ":gravitino1,catalog2:gravitino2")
            .getOrCreate();

    try {
      GravitinoInfoProviderImpl provider = GravitinoInfoProviderImpl.newInstanceForTest();

      // Empty keys should be skipped
      assertThat(provider.getGravitinoCatalog("catalog2")).isEqualTo("gravitino2");
    } finally {
      testSession.stop();
    }
  }

  @Test
  @SneakyThrows
  void testEmptyCatalogMapping() {
    SparkSession testSession =
        SparkSession.builder()
            .master("local[*]")
            .appName("GravitinoCatalogMappingTest")
            .config(SparkGravitinoInfoProvider.metalakeConfigKeyForConnector, "test_metalake")
            .config(SparkGravitinoInfoProvider.useGravitinoConfigKey, "true")
            .config(SparkGravitinoInfoProvider.catalogMappingConfigKey, "")
            .getOrCreate();

    try {
      GravitinoInfoProviderImpl provider = GravitinoInfoProviderImpl.newInstanceForTest();

      // Empty mapping should return original catalog name
      assertThat(provider.getGravitinoCatalog("catalog1")).isEqualTo("catalog1");
      assertThat(provider.getGravitinoCatalog("catalog2")).isEqualTo("catalog2");
    } finally {
      testSession.stop();
    }
  }

  @Test
  @SneakyThrows
  void testCatalogMappingWithTrailingComma() {
    SparkSession testSession =
        SparkSession.builder()
            .master("local[*]")
            .appName("GravitinoCatalogMappingTest")
            .config(SparkGravitinoInfoProvider.metalakeConfigKeyForConnector, "test_metalake")
            .config(SparkGravitinoInfoProvider.useGravitinoConfigKey, "true")
            .config(
                SparkGravitinoInfoProvider.catalogMappingConfigKey,
                "catalog1:gravitino1,catalog2:gravitino2,")
            .getOrCreate();

    try {
      GravitinoInfoProviderImpl provider = GravitinoInfoProviderImpl.newInstanceForTest();

      // Trailing comma should be handled gracefully
      assertThat(provider.getGravitinoCatalog("catalog1")).isEqualTo("gravitino1");
      assertThat(provider.getGravitinoCatalog("catalog2")).isEqualTo("gravitino2");
    } finally {
      testSession.stop();
    }
  }

  @Test
  @SneakyThrows
  void testCatalogMappingWithComplexNames() {
    SparkSession testSession =
        SparkSession.builder()
            .master("local[*]")
            .appName("GravitinoCatalogMappingTest")
            .config(SparkGravitinoInfoProvider.metalakeConfigKeyForConnector, "test_metalake")
            .config(SparkGravitinoInfoProvider.useGravitinoConfigKey, "true")
            .config(
                SparkGravitinoInfoProvider.catalogMappingConfigKey,
                "dev_iceberg_2024:prod_iceberg_v2,local-postgres:gravitino_postgres_prod")
            .getOrCreate();

    try {
      GravitinoInfoProviderImpl provider = GravitinoInfoProviderImpl.newInstanceForTest();

      assertThat(provider.getGravitinoCatalog("dev_iceberg_2024")).isEqualTo("prod_iceberg_v2");
      assertThat(provider.getGravitinoCatalog("local-postgres"))
          .isEqualTo("gravitino_postgres_prod");
    } finally {
      testSession.stop();
    }
  }

  @Test
  @SneakyThrows
  void testCatalogMappingCaseSensitive() {
    SparkSession testSession =
        SparkSession.builder()
            .master("local[*]")
            .appName("GravitinoCatalogMappingTest")
            .config(SparkGravitinoInfoProvider.metalakeConfigKeyForConnector, "test_metalake")
            .config(SparkGravitinoInfoProvider.useGravitinoConfigKey, "true")
            .config(
                SparkGravitinoInfoProvider.catalogMappingConfigKey,
                "Catalog1:Gravitino1,catalog1:gravitino1")
            .getOrCreate();

    try {
      GravitinoInfoProviderImpl provider = GravitinoInfoProviderImpl.newInstanceForTest();

      // Catalog names are case-sensitive
      assertThat(provider.getGravitinoCatalog("Catalog1")).isEqualTo("Gravitino1");
      assertThat(provider.getGravitinoCatalog("catalog1")).isEqualTo("gravitino1");
      assertThat(provider.getGravitinoCatalog("CATALOG1")).isEqualTo("CATALOG1"); // Not mapped
    } finally {
      testSession.stop();
    }
  }
}
