/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent;

import static org.mockserver.model.HttpRequest.request;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.mockserver.configuration.Configuration;
import org.mockserver.integration.ClientAndServer;
import org.slf4j.event.Level;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Slf4j
@Testcontainers
@EnabledIfSystemProperty(named = "spark.version", matches = "(3.*)")
@Tag("integration-test")
public class ColumnLineageIntegrationTest {
  private static final String LOCAL_IP = "127.0.0.1";
  private static final String database = "test";
  private static final String username = "test";
  private static final String password = "test";
  private static final String jdbcQuery =
      "select js1.k, CONCAT(js1.j1, js2.j2) as j from jdbc_source1 js1 join jdbc_source2 js2 on js1.k = js2.k";
  private static final String query =
      "select v.k, concat(j, v) as value from "
          + "jdbc_result j "
          + "join "
          + "(select vs1.k, concat(v1, v2) as v from v2_source_1 vs1 join v2_source_2 vs2 on vs1.k = vs2.k) v "
          + "on j.k = v.k";
  private static Network network = Network.newNetwork();
  private static SparkSession spark;
  public static final int POSTGRES_PORT = 5432;
  private static final int MOCKSERVER_PORT = 1082;

  private static int mappedPort;
  private static ClientAndServer mockServer;

  @Container private static PostgreSQLContainer metastoreContainer;

  static {
    metastoreContainer =
        new PostgreSQLContainer<>(DockerImageName.parse("postgres:13.4-bullseye"))
            .withUsername(username)
            .withPassword(password)
            .withDatabaseName(database)
            .withExposedPorts(POSTGRES_PORT)
            .withFileSystemBind(
                "src/test/resources/column_lineage/init.sql",
                "/docker-entrypoint-initdb.d/init.sql");
  }

  @BeforeAll
  public static void setup() {
    metastoreContainer.start();
    mappedPort = metastoreContainer.getMappedPort(MetastoreTestUtils.POSTGRES_PORT);
    Configuration configuration = new Configuration();
    configuration.logLevel(Level.ERROR);
    mockServer = ClientAndServer.startClientAndServer(configuration, MOCKSERVER_PORT);
    mockServer
        .when(request("/api/v1/lineage"))
        .respond(org.mockserver.model.HttpResponse.response().withStatusCode(201));
    spark = getSparkSession();
    Arrays.asList("v2_source_1", "v2_source_2")
        .forEach(e -> spark.sql("drop table if exists " + e));
    getIcebergTable(spark, 1);
    getIcebergTable(spark, 2);
  }

  @Test
  void columnLevelLineageTest() {
    Dataset<Row> df1 = getTable(spark);
    df1.registerTempTable("jdbc_result");

    spark
        .sql(query)
        .write()
        .format("jdbc")
        .option("url", String.format("jdbc:postgresql://localhost:%s/%s", mappedPort, database))
        .option("dbtable", "test")
        .option("user", username)
        .option("password", password)
        .option("driver", "org.postgresql.Driver")
        .mode(SaveMode.Append)
        .save();

    MockServerUtils.verifyEvents(
        mockServer, "columnLineageJDBCStart.json", "columnLineageJDBCComplete.json");
  }

  @AfterAll
  public static void tearDown() {
    Arrays.asList("v2_source_1", "v2_source_2")
        .forEach(e -> spark.sql("drop table if exists " + e));
    metastoreContainer.stop();
    mockServer.stop();
    spark.close();
  }

  private static SparkSession getSparkSession() {
    return SparkSession.builder()
        .master("local[*]")
        .appName("IcebergIntegrationTest")
        .config("spark.driver.host", LOCAL_IP)
        .config("spark.driver.bindAddress", LOCAL_IP)
        .config("spark.sql.shuffle.partitions", 1)
        .config("spark.sql.warehouse.dir", "file:/tmp/iceberg/")
        .config("spark.driver.extraJavaOptions", "-Dderby.system.home=/tmp/iceberg")
        .config("spark.openlineage.transport.type", "console")
//        .config("spark.openlineage.transport.type", "http")
//        .config(
//            "spark.openlineage.transport.url",
//            "http://localhost:" + mockServer.getPort() + "/api/v1/namespaces/default")
        .config("spark.openlineage.facets.disabled", "spark_unknown;spark.logicalPlan")
        .config("spark.extraListeners", OpenLineageSparkListener.class.getName())
        .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.spark_catalog.type", "hadoop")
        .config("spark.sql.catalog.spark_catalog.warehouse", "/tmp/iceberg")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate();
  }

  private static Dataset<Row> getIcebergTable(SparkSession spark, Integer type) {
    StructType structType = new StructType();
    structType = structType.add("k", DataTypes.IntegerType, false);
    structType = structType.add("v" + type, DataTypes.StringType, false);
    List<Row> rows = new ArrayList<Row>();
    if (type == 1) {
      rows.add(RowFactory.create(1, "va"));
      rows.add(RowFactory.create(2, "vc"));
      rows.add(RowFactory.create(3, "ve"));
    } else {
      rows.add(RowFactory.create(1, "vb"));
      rows.add(RowFactory.create(2, "vd"));
      rows.add(RowFactory.create(3, "vf"));
    }
    Dataset<Row> dataFrame = spark.createDataFrame(rows, structType);
    dataFrame.registerTempTable("temp" + type);
    spark.sql(
        String.format(
            "create table v2_source_%s USING ICEBERG as select * from temp%s", type, type));
    return dataFrame;
  }

  private static Dataset<Row> getTable(SparkSession spark) {
    return spark
        .read()
        .format("jdbc")
        .option("url", String.format("jdbc:postgresql://localhost:%s/%s", mappedPort, database))
        .option("query", jdbcQuery)
        .option("user", username)
        .option("password", password)
        .option("driver", "org.postgresql.Driver")
        .load();
  }
}
