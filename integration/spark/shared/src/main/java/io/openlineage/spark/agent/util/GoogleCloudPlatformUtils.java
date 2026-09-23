/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.util;

import java.util.Map;
import java.util.Optional;
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

  public static Optional<Map<String, String>> getDataprocMetastoreProperties(SparkConf conf) {
    if (conf.contains("spark.dataproc.metastore.service.short.name")
        && conf.contains("spark.dataproc.metastore.project-id")
        && conf.contains("spark.dataproc.metastore.location")) {
      return Optional.of(
          Map.of(
              "gcp_project_id", conf.get("spark.dataproc.metastore.project-id"),
              "gcp_location", conf.get("spark.dataproc.metastore.location"),
              "gcp_instance_id", conf.get("spark.dataproc.metastore.service.short.name")));
    }
    return Optional.empty();
  }
}
