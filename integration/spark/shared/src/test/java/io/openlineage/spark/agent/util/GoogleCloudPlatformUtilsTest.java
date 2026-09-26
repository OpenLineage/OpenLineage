/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.util;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mockStatic;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.spark.SparkConf;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

class GoogleCloudPlatformUtilsTest {

  String METASTORE_HIVE_CONF_STRING = "METASTORE_CLIENT_FACTORY_CLASS";
  String METASTORE_CLASS_KEY = "spark.hive.metastore.client.factory.class";
  String VALID_METASTORE_CLASS_VALUE =
      "com.google.cloud.bigquery.metastore.client.BigLakeMetastoreClientFactory";
  String INVALID_METASTORE_CLASS_VALUE = "test";

  @Test
  void bigLakeMetastoreAbsentCustomHiveAbsent() {
    SparkConf conf = new SparkConf();
    conf.set(METASTORE_CLASS_KEY, INVALID_METASTORE_CLASS_VALUE);
    try (MockedStatic<HiveConf.ConfVars> confVars = mockStatic(HiveConf.ConfVars.class)) {
      confVars
          .when(() -> HiveConf.ConfVars.valueOf(METASTORE_HIVE_CONF_STRING))
          .thenThrow(new IllegalArgumentException());
      assertFalse(GoogleCloudPlatformUtils.isBigLakeHiveCatalog(conf));
    }
  }

  @Test
  void bigLakeMetastorePresentCustomHiveAbsent() {
    SparkConf conf = new SparkConf();
    conf.set(METASTORE_CLASS_KEY, VALID_METASTORE_CLASS_VALUE);
    try (MockedStatic<HiveConf.ConfVars> confVars = mockStatic(HiveConf.ConfVars.class)) {
      confVars
          .when(() -> HiveConf.ConfVars.valueOf(METASTORE_HIVE_CONF_STRING))
          .thenThrow(new IllegalArgumentException());
      assertFalse(GoogleCloudPlatformUtils.isBigLakeHiveCatalog(conf));
    }
  }

  @Test
  void bigLakeMetastoreAbsentCustomHivePresent() {
    SparkConf conf = new SparkConf();
    conf.set(METASTORE_CLASS_KEY, INVALID_METASTORE_CLASS_VALUE);
    try (MockedStatic<HiveConf.ConfVars> confVars = mockStatic(HiveConf.ConfVars.class)) {
      confVars.when(() -> HiveConf.ConfVars.valueOf(METASTORE_HIVE_CONF_STRING)).thenReturn(null);
      assertFalse(GoogleCloudPlatformUtils.isBigLakeHiveCatalog(conf));
    }
  }

  @Test
  void bigLakeMetastorePresentCustomHivePresent() {
    SparkConf conf = new SparkConf();
    conf.set(METASTORE_CLASS_KEY, VALID_METASTORE_CLASS_VALUE);
    try (MockedStatic<HiveConf.ConfVars> confVars = mockStatic(HiveConf.ConfVars.class)) {
      confVars.when(() -> HiveConf.ConfVars.valueOf(METASTORE_HIVE_CONF_STRING)).thenReturn(null);
      assertTrue(GoogleCloudPlatformUtils.isBigLakeHiveCatalog(conf));
    }
  }
}
