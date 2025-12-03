/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.util;

import io.openlineage.client.utils.gravitino.GravitinoInfoProviderImpl;
import io.openlineage.client.utils.gravitino.SparkGravitinoInfoProvider;
import lombok.SneakyThrows;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.SparkSession$;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class SparkGravitinoInfoProviderTest {

  @BeforeAll
  @SneakyThrows
  public static void beforeAll() {
    cleanUpExistingSession();
  }

  @AfterAll
  @SneakyThrows
  public static void afterAll() {
    cleanUpExistingSession();
    // Clear the singleton cache to avoid affecting other tests
    GravitinoInfoProviderImpl.getInstance().clearCache();
  }

  @SneakyThrows
  @Test
  public void testGetMetalakeName() {
    GravitinoInfoProviderImpl provider = GravitinoInfoProviderImpl.newInstanceForTest();

    SparkSession.builder()
        .master("local[*]")
        .appName("test")
        .config(SparkGravitinoInfoProvider.metalakeConfigKeyForConnector, "metalake_name")
        .getOrCreate();
    Assertions.assertEquals("metalake_name", provider.getMetalakeName());

    provider = GravitinoInfoProviderImpl.newInstanceForTest();
    cleanUpExistingSession();
    SparkSession.builder()
        .master("local[*]")
        .appName("test")
        .config(SparkGravitinoInfoProvider.metalakeConfigKeyForFS, "metalake_name2")
        .getOrCreate();
    Assertions.assertEquals("metalake_name2", provider.getMetalakeName());

    provider = GravitinoInfoProviderImpl.newInstanceForTest();
    cleanUpExistingSession();
    SparkSession.builder()
        .master("local[*]")
        .appName("test")
        .config(SparkGravitinoInfoProvider.metalakeConfigKeyForConnector, "metalake_name3")
        .config(SparkGravitinoInfoProvider.metalakeConfigKeyForFS, "metalake_name4")
        .getOrCreate();
    Assertions.assertEquals("metalake_name3", provider.getMetalakeName());

    GravitinoInfoProviderImpl provider2 = GravitinoInfoProviderImpl.newInstanceForTest();
    cleanUpExistingSession();
    SparkSession.builder().master("local[*]").appName("test").getOrCreate();
    Assertions.assertThrowsExactly(RuntimeException.class, () -> provider2.getMetalakeName());
  }

  @SneakyThrows
  @Test
  public void testGetCatalogName() {
    GravitinoInfoProviderImpl provider = GravitinoInfoProviderImpl.newInstanceForTest();

    SparkSession.builder()
        .master("local[*]")
        .appName("test")
        .config(
            SparkGravitinoInfoProvider.catalogMappingConfigKey,
            "catalog1:gravitino1,catalog2:gravitino2")
        .getOrCreate();

    Assertions.assertEquals("gravitino1", provider.getGravitinoCatalog("catalog1"));
    Assertions.assertEquals("gravitino2", provider.getGravitinoCatalog("catalog2"));
    Assertions.assertEquals("catalog3", provider.getGravitinoCatalog("catalog3"));

    provider = GravitinoInfoProviderImpl.newInstanceForTest();
    cleanUpExistingSession();

    SparkSession.builder().master("local[*]").appName("test").getOrCreate();

    Assertions.assertEquals("catalog1", provider.getGravitinoCatalog("catalog1"));
    Assertions.assertEquals("catalog2", provider.getGravitinoCatalog("catalog2"));
    Assertions.assertEquals("catalog3", provider.getGravitinoCatalog("catalog3"));
  }

  @SneakyThrows
  @Test
  public void testUseGravitinoIdentifier() {
    GravitinoInfoProviderImpl provider = GravitinoInfoProviderImpl.newInstanceForTest();
    SparkSession.builder()
        .master("local[*]")
        .appName("test")
        .config(SparkGravitinoInfoProvider.useGravitinoConfigKey, "true")
        .getOrCreate();
    Assertions.assertEquals(true, provider.useGravitinoIdentifier());

    provider = GravitinoInfoProviderImpl.newInstanceForTest();
    cleanUpExistingSession();
    SparkSession.builder().master("local[*]").appName("test2").getOrCreate();
    Assertions.assertEquals(false, provider.useGravitinoIdentifier());

    provider = GravitinoInfoProviderImpl.newInstanceForTest();
    cleanUpExistingSession();
    SparkSession.builder()
        .master("local[*]")
        .appName("test")
        .config(SparkGravitinoInfoProvider.useGravitinoConfigKey, "false")
        .getOrCreate();
    Assertions.assertEquals(false, provider.useGravitinoIdentifier());
  }

  private static void cleanUpExistingSession() {
    SparkSession$.MODULE$.cleanupAnyExistingSession();
  }
}
