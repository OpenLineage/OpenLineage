/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent;

import static org.assertj.core.api.Assertions.assertThat;

import io.openlineage.spark.agent.util.DerbyUtils;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
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
@Disabled(
    "This test is feature incomplete, and needs to be fixed. It doesn't actually test any OpenLineage code.")
// FIXME: This test does not configure the OpenLineageSparkListener, and thus does not make any
//  assertions about the events that the listener would emit.
class MetastoreHive3Test {
  private static final String database = "hive3";
  private static final String table = "test";
  private static final Network network = Network.newNetwork();
  private static SparkSession spark;
  private static FileSystem fs;

  @Container
  private static final PostgreSQLContainer<?> metastoreContainer =
      SparkContainerUtils.makeMetastoreContainer(network);

  @BeforeAll
  public static void setup() {
    DerbyUtils.loadSystemProperty(MetastoreHive3Test.class.getName());
    metastoreContainer.start();
    spark =
        SparkSession.builder()
            .config(
                MetastoreTestUtils.getCommonSparkConf(
                    "MetastoreHive3Test",
                    "metastore31",
                    metastoreContainer.getMappedPort(MetastoreTestUtils.POSTGRES_PORT),
                    true))
            .enableHiveSupport()
            .getOrCreate();
    fs = MetastoreTestUtils.getFileSystem(spark);
  }

  @AfterAll
  public static void tearDown() {
    DerbyUtils.clearDerbyProperty();
    metastoreContainer.stop();
    MetastoreTestUtils.removeDatabaseFiles(database, fs);
    spark.close();
  }

  @BeforeEach
  void reset() {
    executeSql("DROP TABLE IF EXISTS %s.%s", database, table);
    executeSql("DROP DATABASE IF EXISTS %s", database);
    MetastoreTestUtils.removeDatabaseFiles(database, fs);
  }

  @Test
  @Tag("iceberg")
  void IcebergTablesTest() {
    executeSql("create database if not exists %s", database);
    executeSql("drop table if exists %s.%s", database, table);
    executeSql(
        "create external table %s.%s (id int, value string) USING iceberg location '%s'",
        database, table, MetastoreTestUtils.getTableLocation(database, table));
    executeSql("insert into table %s.%s VALUES (1, 'value1'), (2, 'value2')", database, table);
    Dataset<Row> rowDataset = executeSql(String.format("select * from %s.%s", database, table));
    List<Row> rows = rowDataset.collectAsList();
    assertThat(rows).hasSize(2);
    assertThat(rows.get(0).get(0)).isEqualTo(1);
  }

  public static Dataset<Row> executeSql(String query, String... params) {
    return spark.sql(String.format(query, params));
  }
}
