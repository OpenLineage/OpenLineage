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
      String name = HiveConf.ConfVars.valueOf("spark.hive.metastore.client.factory.class").name();
      return conf.getOption(name)
          .exists("com.google.cloud.spark.biglake.hive.BigLakeHiveClientFactory"::equals);
    } catch (IllegalArgumentException e) {
      return false;
    }
  }
}
