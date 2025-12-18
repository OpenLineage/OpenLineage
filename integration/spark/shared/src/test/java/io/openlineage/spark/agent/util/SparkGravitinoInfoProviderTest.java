/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.util;

import io.openlineage.client.utils.gravitino.GravitinoInfoProviderImpl;
import io.openlineage.spark.agent.util.SparkGravitinoInfoProvider;
import lombok.SneakyThrows;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.SparkSession$;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class SparkGravitinoInfoProviderTest {

  private static final String LOCAL_MASTER = "local[*]";
  private static final String TEST_APP_NAME = "test";
  private static final String CATALOG3 = "catalog3";

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
  void testGetMetalakeName() {
    GravitinoInfoProviderImpl provider = GravitinoInfoProviderImpl.newInstanceForTest();

    SparkSession.builder()
        .master(LOCAL_MASTER)
        .appName(TEST_APP_NAME)
        .config(SparkGravitinoInfoProvider.metalakeConfigKeyForConnector, "metalake_name")
        .getOrCreate();
    Assertions.assertEquals("metalake_name", provider.getMetalakeName());

    provider = GravitinoInfoProviderImpl.newInstanceForTest();
    cleanUpExistingSession();
    SparkSession.builder()
        .master(LOCAL_MASTER)
        .appName(TEST_APP_NAME)
        .config(SparkGravitinoInfoProvider.metalakeConfigKeyForFS, "metalake_name2")
        .getOrCreate();
    Assertions.assertEquals("metalake_name2", provider.getMetalakeName());

    provider = GravitinoInfoProviderImpl.newInstanceForTest();
    cleanUpExistingSession();
    SparkSession.builder()
        .master(LOCAL_MASTER)
        .appName(TEST_APP_NAME)
        .config(SparkGravitinoInfoProvider.metalakeConfigKeyForConnector, "metalake_name3")
        .config(SparkGravitinoInfoProvider.metalakeConfigKeyForFS, "metalake_name4")
        .getOrCreate();
    Assertions.assertEquals("metalake_name3", provider.getMetalakeName());

    GravitinoInfoProviderImpl provider2 = GravitinoInfoProviderImpl.newInstanceForTest();
    cleanUpExistingSession();
    SparkSession.builder().master(LOCAL_MASTER).appName(TEST_APP_NAME).getOrCreate();
    Assertions.assertThrowsExactly(RuntimeException.class, () -> provider2.getMetalakeName());
  }



  @SneakyThrows
  @Test
  void testUseGravitinoIdentifier() {
    GravitinoInfoProviderImpl provider = GravitinoInfoProviderImpl.newInstanceForTest();
    SparkSession.builder()
        .master(LOCAL_MASTER)
        .appName(TEST_APP_NAME)
        .config(SparkGravitinoInfoProvider.useGravitinoConfigKey, "true")
        .getOrCreate();
    Assertions.assertTrue(provider.useGravitinoIdentifier());

    provider = GravitinoInfoProviderImpl.newInstanceForTest();
    cleanUpExistingSession();
    SparkSession.builder().master(LOCAL_MASTER).appName("test2").getOrCreate();
    Assertions.assertFalse(provider.useGravitinoIdentifier());

    provider = GravitinoInfoProviderImpl.newInstanceForTest();
    cleanUpExistingSession();
    SparkSession.builder()
        .master(LOCAL_MASTER)
        .appName(TEST_APP_NAME)
        .config(SparkGravitinoInfoProvider.useGravitinoConfigKey, "false")
        .getOrCreate();
    Assertions.assertFalse(provider.useGravitinoIdentifier());
  }

  private static void cleanUpExistingSession() {
    SparkSession$.MODULE$.cleanupAnyExistingSession();
  }
}
