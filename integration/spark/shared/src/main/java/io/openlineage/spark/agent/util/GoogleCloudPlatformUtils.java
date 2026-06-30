/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.util;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.spark.SparkConf;

@Slf4j
public class GoogleCloudPlatformUtils {
  public static boolean isBigLakeHiveCatalog(SparkConf conf) {
    try {
      // To use Big Lake Hive Catalog you have to use custom implementation of HiveConf that
      // contains this enum
      // if the enum is present, you can set the custom client factory
      HiveConf.ConfVars.valueOf("METASTORE_CLIENT_FACTORY_CLASS");
      log.debug("detected custom Metastore Client Factory class");
      return conf.getOption("spark.hive.metastore.client.factory.class")
          .exists(
              "com.google.cloud.bigquery.metastore.client.BigLakeMetastoreClientFactory"::equals);
    } catch (IllegalArgumentException e) {
      return false;
    }
  }
}
