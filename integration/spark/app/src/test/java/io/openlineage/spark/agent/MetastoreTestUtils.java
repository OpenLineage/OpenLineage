/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent;

import static io.openlineage.spark.agent.SparkContainerProperties.SCALA_BINARY_VERSION;
import static io.openlineage.spark.agent.SparkContainerProperties.SPARK_VERSION;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

@Slf4j
public class MetastoreTestUtils {
  private static final String LOCAL_IP = "127.0.0.1";
  private static final String GS_BUCKET_NAME =
      Optional.ofNullable(System.getenv("GCLOUD_METASTORE_BUCKET_NAME"))
          .orElse("openlineage-ci-testing");
  private static final URI BASE_URI = URI.create("gs://" + GS_BUCKET_NAME + "/");
  public static final URI WAREHOUSE_URI = BASE_URI.resolve("warehouse/");
  public static final String BASE_PATH =
      WAREHOUSE_URI
          .resolve("spark-" + SPARK_VERSION + "/")
          .resolve("scala-" + SCALA_BINARY_VERSION)
          .toString()
          .replace(".", "-");
  private static final File GCLOUD_KEY_FILE =
      Paths.get("build/gcloud/gcloud-service-key.json").toAbsolutePath().toFile();
  private static final Map<String, String> GOOGLE_SA_PROPERTIES = parseGoogleSAProperties();
  public static final int POSTGRES_PORT = 5432;

  public static SparkConf getCommonSparkConf(
      String appName, String metastoreName, int mappedPort, Boolean isIceberg) {
    SparkConf conf =
        new SparkConf()
            .setAppName(appName + SPARK_VERSION)
            .setMaster("local[*]")
            .set("spark.driver.host", LOCAL_IP)
            .set("spark.ui.enabled", "false")
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
    ObjectMapper objectMapper = new ObjectMapper();
    Map<String, String> gcsProperties;
    try {
      gcsProperties = objectMapper.readValue(GCLOUD_KEY_FILE, Map.class);
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
