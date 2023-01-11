/*
/* Copyright 2018-2023 contributors to the OpenLineage project
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
  private static final String database = "hive2";
  private static final String table = "test";
  private static Network network = Network.newNetwork();
  private static SparkSession spark;
  private static FileSystem fs;

  @Container
  private static PostgreSQLContainer metastoreContainer =
      SparkContainerUtils.makeMetastoreContainer(network);

  private static int mappedPort;

  @BeforeAll
  public static void setup() {
    metastoreContainer.start();
    mappedPort = metastoreContainer.getMappedPort(MetastoreTestUtils.POSTGRES_PORT);
    spark =
        SparkSession.builder()
            .config(
                MetastoreTestUtils.getCommonSparkConf(
                    "MetastoreHive2Test",
                    "metastore23",
                    metastoreContainer.getMappedPort(MetastoreTestUtils.POSTGRES_PORT),
                    false))
            .enableHiveSupport()
            .getOrCreate();
    fs = MetastoreTestUtils.getFileSystem(spark);

    MetastoreTestUtils.removeDatabaseFiles(database, fs);
    executeSql("DROP TABLE IF EXISTS %s.%s", database, table);
    executeSql("DROP DATABASE IF EXISTS %s", database);
  }

  @AfterAll
  public static void tearDown() {
    metastoreContainer.stop();
    MetastoreTestUtils.removeDatabaseFiles(database, fs);
    spark.close();
  }

  @Test
  void IcebergTablesTest() {
    executeSql("create database if not exists %s", database);
    executeSql("drop table if exists %s.%s", database, table);
    executeSql(
        "create external table %s.%s (id int, value string) location '%s'",
        database, table, MetastoreTestUtils.getTableLocation(database, table));
    executeSql("insert into table %s.%s VALUES (1, 'value1'), (2, 'value2')", database, table);
    Dataset<Row> rowDataset = executeSql(String.format("select * from %s.%s", database, table));
    List<Row> rows = rowDataset.collectAsList();
    assertThat(rows.size()).isEqualTo(2);
    assertThat(rows.get(0).get(0)).isEqualTo(1);
  }

  public static Dataset<Row> executeSql(String query, String... params) {
    return spark.sql(String.format(query, params));
  }
}
