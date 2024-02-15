/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Slf4j
@Tag("integration-test")
@Testcontainers
@EnabledIfEnvironmentVariable(named = "CI", matches = "true")
@EnabledIfSystemProperty(named = "spark.version", matches = "(3.*)")
public class MetastoreHive2Test {
  private static final String SPARK_VERSION = System.getProperty("spark.version");
  private static final String SCALA_BINARY_VERSION = System.getProperty("scala.binary.version");
  private static final String DATABASE = "hive3";
  private static final String TABLE =
      String.format("test_%s_%s", SPARK_VERSION, SCALA_BINARY_VERSION).replace(".", "_");
  private static final Network NETWORK = Network.newNetwork();
  @Container
  private static final PostgreSQLContainer<?> METASTORE_CONTAINER =
      SparkContainerUtils.makeMetastoreContainer(NETWORK);
  private static SparkSession spark;
  private static FileSystem fs;

  @BeforeAll
  public static void setup() {
    METASTORE_CONTAINER.start();
    spark =
        SparkSession.builder()
            .config(
                MetastoreTestUtils.getCommonSparkConf(
                    "MetastoreHive2Test",
                    "metastore23",
                    METASTORE_CONTAINER.getMappedPort(MetastoreTestUtils.POSTGRES_PORT),
                    false))
            .enableHiveSupport()
            .getOrCreate();
    fs = MetastoreTestUtils.getFileSystem(spark);

    MetastoreTestUtils.removeDatabaseFiles(DATABASE, fs);
    executeSql("DROP TABLE IF EXISTS %s.%s", DATABASE, TABLE);
    executeSql("DROP DATABASE IF EXISTS %s", DATABASE);
  }

  @AfterAll
  public static void tearDown() {
    METASTORE_CONTAINER.stop();
    MetastoreTestUtils.removeDatabaseFiles(DATABASE, fs);
    spark.close();
  }

  @Test
  void IcebergTablesTest() {
    executeSql("create database if not exists %s", DATABASE);
    executeSql("drop table if exists %s.%s", DATABASE, TABLE);
    executeSql(
        "create external table %s.%s (id int, value string) location '%s'",
        DATABASE, TABLE, MetastoreTestUtils.getTableLocation(DATABASE, TABLE));
    executeSql("insert into table %s.%s VALUES (1, 'value1'), (2, 'value2')", DATABASE, TABLE);
    Dataset<Row> rowDataset = executeSql(String.format("select * from %s.%s", DATABASE, TABLE));
    List<Row> rows = rowDataset.collectAsList();
    assertThat(rows.size()).isEqualTo(2);
    assertThat(rows.get(0).get(0)).isEqualTo(1);
  }

  public static Dataset<Row> executeSql(String query, String... params) {
    return spark.sql(String.format(query, params));
  }
}
