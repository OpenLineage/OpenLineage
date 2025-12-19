/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.util;

import static org.assertj.core.api.Assertions.assertThat;

import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.client.utils.gravitino.GravitinoInfo;
import io.openlineage.client.utils.gravitino.GravitinoInfoProviderImpl;
import java.util.Optional;
import lombok.SneakyThrows;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.SparkSession$;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

@SuppressWarnings("PMD.AvoidDuplicateLiterals")
class GravitinoUtilsTest {

  @AfterEach
  void tearDown() {
    SparkSession$.MODULE$.cleanupAnyExistingSession();
    GravitinoInfoProviderImpl.getInstance().clearCache();
  }

  @Test
  @SneakyThrows
  void testGetGravitinoDatasetIdentifierFromIdentifier() {
    SparkSession testSession =
        SparkSession.builder()
            .master("local[*]")
            .appName("GravitinoUtilsTest")
            .config(SparkGravitinoInfoProvider.metalakeConfigKeyForConnector, "test_metalake")
            .config(SparkGravitinoInfoProvider.uriConfigKey, "http://localhost:8090")
            .getOrCreate();

    try {
      GravitinoInfo gravitinoInfo =
          GravitinoInfo.builder()
              .metalake(Optional.of("test_metalake"))
              .uri(Optional.of("http://localhost:8090"))
              .build();
      String catalogName = "iceberg_catalog";
      String[] defaultNamespace = new String[] {"db"};
      Identifier identifier = Identifier.of(new String[] {"db"}, "table");

      DatasetIdentifier datasetIdentifier =
          GravitinoUtils.getGravitinoDatasetIdentifier(
              gravitinoInfo, catalogName, defaultNamespace, identifier);

      assertThat(datasetIdentifier)
          .hasFieldOrPropertyWithValue(
              "namespace", "http://localhost:8090/api/metalakes/test_metalake")
          .hasFieldOrPropertyWithValue("name", "iceberg_catalog.db.table");
    } finally {
      testSession.stop();
    }
  }

  @Test
  @SneakyThrows
  void testGetGravitinoDatasetIdentifierWithEmptyIdentifierNamespace() {
    SparkSession testSession =
        SparkSession.builder()
            .master("local[*]")
            .appName("GravitinoUtilsTest")
            .config(SparkGravitinoInfoProvider.metalakeConfigKeyForConnector, "test_metalake")
            .config(SparkGravitinoInfoProvider.uriConfigKey, "http://localhost:8090")
            .getOrCreate();

    try {
      GravitinoInfo gravitinoInfo =
          GravitinoInfo.builder()
              .metalake(Optional.of("test_metalake"))
              .uri(Optional.of("http://localhost:8090"))
              .build();
      String catalogName = "iceberg_catalog";
      String[] defaultNamespace = new String[] {"default_db"};
      // Identifier with empty namespace - should use default
      Identifier identifier = Identifier.of(new String[] {}, "table");

      DatasetIdentifier datasetIdentifier =
          GravitinoUtils.getGravitinoDatasetIdentifier(
              gravitinoInfo, catalogName, defaultNamespace, identifier);

      assertThat(datasetIdentifier)
          .hasFieldOrPropertyWithValue(
              "namespace", "http://localhost:8090/api/metalakes/test_metalake")
          .hasFieldOrPropertyWithValue("name", "iceberg_catalog.default_db.table");
    } finally {
      testSession.stop();
    }
  }

  @Test
  @SneakyThrows
  void testGetGravitinoDatasetIdentifierWithNestedNamespace() {
    SparkSession testSession =
        SparkSession.builder()
            .master("local[*]")
            .appName("GravitinoUtilsTest")
            .config(SparkGravitinoInfoProvider.metalakeConfigKeyForConnector, "test_metalake")
            .config(SparkGravitinoInfoProvider.uriConfigKey, "http://localhost:8090")
            .getOrCreate();

    try {
      GravitinoInfo gravitinoInfo =
          GravitinoInfo.builder()
              .metalake(Optional.of("test_metalake"))
              .uri(Optional.of("http://localhost:8090"))
              .build();
      String catalogName = "iceberg_catalog";
      String[] defaultNamespace = new String[] {"level1"};
      Identifier identifier = Identifier.of(new String[] {"level1", "level2", "level3"}, "table");

      DatasetIdentifier datasetIdentifier =
          GravitinoUtils.getGravitinoDatasetIdentifier(
              gravitinoInfo, catalogName, defaultNamespace, identifier);

      assertThat(datasetIdentifier)
          .hasFieldOrPropertyWithValue(
              "namespace", "http://localhost:8090/api/metalakes/test_metalake")
          .hasFieldOrPropertyWithValue("name", "iceberg_catalog.level1.level2.level3.table");
    } finally {
      testSession.stop();
    }
  }

  @Test
  @SneakyThrows
  void testGetGravitinoDatasetIdentifierWithComplexNames() {
    SparkSession testSession =
        SparkSession.builder()
            .master("local[*]")
            .appName("GravitinoUtilsTest")
            .config(SparkGravitinoInfoProvider.metalakeConfigKeyForConnector, "prod_metalake")
            .config(SparkGravitinoInfoProvider.uriConfigKey, "https://gravitino.prod.example.com")
            .getOrCreate();

    try {
      GravitinoInfo gravitinoInfo =
          GravitinoInfo.builder()
              .metalake(Optional.of("prod_metalake"))
              .uri(Optional.of("https://gravitino.prod.example.com"))
              .build();
      String catalogName = "iceberg_prod_2024";
      String[] defaultNamespace = new String[] {"analytics_v2"};
      Identifier identifier = Identifier.of(new String[] {"analytics_v2"}, "user_events_2024_q1");

      DatasetIdentifier datasetIdentifier =
          GravitinoUtils.getGravitinoDatasetIdentifier(
              gravitinoInfo, catalogName, defaultNamespace, identifier);

      assertThat(datasetIdentifier)
          .hasFieldOrPropertyWithValue(
              "namespace", "https://gravitino.prod.example.com/api/metalakes/prod_metalake")
          .hasFieldOrPropertyWithValue(
              "name", "iceberg_prod_2024.analytics_v2.user_events_2024_q1");
    } finally {
      testSession.stop();
    }
  }
}
