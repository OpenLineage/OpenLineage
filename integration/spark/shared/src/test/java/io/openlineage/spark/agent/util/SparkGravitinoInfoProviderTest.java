/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.util;

import io.openlineage.client.utils.gravitino.GravitinoInfoManager;
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
    GravitinoInfoManager.getInstance().clearCache();
  }

  @SneakyThrows
  @Test
  void testGetMetalakeName() {
    GravitinoInfoManager provider = GravitinoInfoManager.newInstanceForTest();

    SparkSession.builder()
        .master(LOCAL_MASTER)
        .appName(TEST_APP_NAME)
        .config(SparkGravitinoInfoProvider.metalakeConfigKeyForConnector, "metalake_name")
        .getOrCreate();
    Assertions.assertEquals("metalake_name", provider.getMetalakeName());

    provider = GravitinoInfoManager.newInstanceForTest();
    cleanUpExistingSession();
    SparkSession.builder()
        .master(LOCAL_MASTER)
        .appName(TEST_APP_NAME)
        .config(SparkGravitinoInfoProvider.metalakeConfigKeyForFS, "metalake_name2")
        .getOrCreate();
    Assertions.assertEquals("metalake_name2", provider.getMetalakeName());

    provider = GravitinoInfoManager.newInstanceForTest();
    cleanUpExistingSession();
    SparkSession.builder()
        .master(LOCAL_MASTER)
        .appName(TEST_APP_NAME)
        .config(SparkGravitinoInfoProvider.metalakeConfigKeyForConnector, "metalake_name3")
        .config(SparkGravitinoInfoProvider.metalakeConfigKeyForFS, "metalake_name4")
        .getOrCreate();
    Assertions.assertEquals("metalake_name3", provider.getMetalakeName());

    GravitinoInfoManager provider2 = GravitinoInfoManager.newInstanceForTest();
    cleanUpExistingSession();
    SparkSession.builder().master(LOCAL_MASTER).appName(TEST_APP_NAME).getOrCreate();
    Assertions.assertThrowsExactly(IllegalStateException.class, () -> provider2.getMetalakeName());
  }

  private static void cleanUpExistingSession() {
    SparkSession$.MODULE$.cleanupAnyExistingSession();
  }
}
