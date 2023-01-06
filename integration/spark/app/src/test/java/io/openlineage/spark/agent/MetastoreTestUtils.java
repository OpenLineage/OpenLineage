/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

public class MetastoreTestUtils {
  private static final String LOCAL_IP = "127.0.0.1";
  private static final String VERSION = System.getProperty("spark.version");
  private static final String BASE_PATH = "gs://openlineage-ci-testing/warehouse/" + VERSION;
  private static final String GCLOUD_KEY = "GCLOUD_SERVICE_KEY";
  private static final Map<String, String> GOOGLE_SA_PROPERTIES = parseGoogleSAProperties();

  public static final int POSTGRES_PORT = 5432;

  public static SparkConf getCommonSparkConf(
      String appName, String metastoreName, int mappedPort, Boolean isIceberg) {
    SparkConf conf =
        new SparkConf()
            .setAppName(appName + VERSION)
            .setMaster("local[*]")
            .set("spark.driver.host", LOCAL_IP)
            .set("org.jpox.autoCreateSchema", "true")
            .set(
                "javax.jdo.option.ConnectionURL",
                String.format("jdbc:postgresql://localhost:%d/%s", mappedPort, metastoreName))
            .set("javax.jdo.option.ConnectionDriverName", "org.postgresql.Driver")
            .set("javax.jdo.option.ConnectionUserName", "hiveuser")
            .set("javax.jdo.option.ConnectionPassword", "password")
            .set("spark.sql.warehouse.dir", BASE_PATH)
            .set("spark.hadoop.google.cloud.project.id", GOOGLE_SA_PROPERTIES.get("project_id"))
            .set("spark.hadoop.google.cloud.auth.service.account.enable", "true")
            .set(
                "fs.gs.auth.service.account.private.key.id",
                GOOGLE_SA_PROPERTIES.get("private_key_id"))
            .set("fs.gs.auth.service.account.private.key", GOOGLE_SA_PROPERTIES.get("private_key"))
            .set("fs.gs.auth.service.account.email", GOOGLE_SA_PROPERTIES.get("client_email"))
            .set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
            .set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS");

    if (isIceberg) {
      conf.set("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog")
          .set("spark.sql.catalog.spark_catalog.type", "hive")
          .set(
              "spark.sql.extensions",
              "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions");
    }
    return conf;
  }

  public static Map<String, String> parseGoogleSAProperties() {
    String json = System.getenv(GCLOUD_KEY);
    ObjectMapper objectMapper = new ObjectMapper();
    Map<String, String> gcsProperties;
    try {
      gcsProperties = objectMapper.readValue(json, Map.class);
    } catch (IOException e) {
      throw new RuntimeException("Couldn't parse properties from GCLOUD_KEY", e);
    }
    return gcsProperties;
  }

  public static Configuration getFileSystemConf(SparkSession spark) {
    Map<String, String> saProperties = MetastoreTestUtils.parseGoogleSAProperties();
    Configuration conf = spark.sparkContext().hadoopConfiguration();
    conf.set("fs.gs.auth.service.account.private.key.id", saProperties.get("private_key_id"));
    conf.set("fs.gs.auth.service.account.private.key", saProperties.get("private_key"));
    conf.set("fs.gs.auth.service.account.email", saProperties.get("client_email"));
    conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem");
    conf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS");
    return conf;
  }

  public static void removeDatabaseFiles(String database, FileSystem fs) {
    try {
      Path path = new Path(BASE_PATH, database);
      if (fs.exists(path)) {
        fs.delete(path, true);
      }
    } catch (IOException e) {
      throw new RuntimeException("Couldn't remove directory " + database, e);
    }
  }

  public static String getTableLocation(String database, String table) {
    return String.format("%s/%s/%s", BASE_PATH, database, table);
  }

  public static FileSystem getFileSystem(SparkSession spark) {
    try {
      return new Path(BASE_PATH).getFileSystem(MetastoreTestUtils.getFileSystemConf(spark));
    } catch (IOException e) {
      throw new RuntimeException("Couldn't get file system", e);
    }
  }
}
