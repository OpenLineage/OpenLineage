/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent;

import static io.openlineage.spark.agent.MockServerUtils.verifyEvents;
import static org.mockserver.model.HttpRequest.request;

import com.google.common.collect.ImmutableList;
import java.io.File;
import java.util.Arrays;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.SparkSession$;
import org.apache.spark.sql.types.LongType$;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockserver.configuration.Configuration;
import org.mockserver.integration.ClientAndServer;
import org.slf4j.event.Level;

@Tag("integration-test")
@Tag("iceberg")
@Slf4j
public class SparkIcebergIntegrationTest {

  @SuppressWarnings("PMD")
  private static final String LOCAL_IP = "127.0.0.1";

  private static final int MOCKSERVER_PORT = 1081;

  private static ClientAndServer mockServer;

  static SparkSession spark;

  @BeforeAll
  @SneakyThrows
  public static void beforeAll() {
    SparkSession$.MODULE$.cleanupAnyExistingSession();
    FileUtils.deleteDirectory(new File("/tmp/iceberg/"));
    Configuration configuration = new Configuration();
    configuration.logLevel(Level.ERROR);
    mockServer = ClientAndServer.startClientAndServer(configuration, MOCKSERVER_PORT);
    mockServer
        .when(request("/api/v1/lineage"))
        .respond(org.mockserver.model.HttpResponse.response().withStatusCode(201));
  }

  @AfterAll
  @SneakyThrows
  public static void afterAll() {
    SparkSession$.MODULE$.cleanupAnyExistingSession();
    mockServer.stop();
  }

  @BeforeEach
  @SneakyThrows
  public void beforeEach() {
    spark =
        SparkSession.builder()
            .master("local[*]")
            .appName("IcebergIntegrationTest")
            .config("spark.driver.host", LOCAL_IP)
            .config("spark.driver.bindAddress", LOCAL_IP)
            .config("spark.sql.shuffle.partitions", 1)
            .config("spark.sql.warehouse.dir", "file:/tmp/iceberg/")
            .config("spark.driver.extraJavaOptions", "-Dderby.system.home=/tmp/iceberg")
            .config("spark.openlineage.transport.type", "http")
            .config(
                "spark.openlineage.transport.url",
                "http://localhost:" + mockServer.getPort() + "/api/v1/namespaces/iceberg-namespace")
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

    spark.sparkContext().setLogLevel("WARN");
  }

  @Test
  void testAlterTable() {
    clearTables("alter_table_test");
    spark.sql("CREATE TABLE alter_table_test (a string, b string) USING iceberg");
    spark.sql("INSERT INTO alter_table_test VALUES ('a', 'b')");
    spark.sql("ALTER TABLE alter_table_test RENAME COLUMN b TO c");

    verifyEvents(
        mockServer, "pysparkV2AlterTableStartEvent.json", "pysparkV2AlterTableCompleteEvent.json");
  }

  @Test
  void testWriteTableVersion() {
    spark.sql("CREATE TABLE table (a int, b int) USING iceberg");
    spark.sql("INSERT INTO table VALUES (1, 2)");

    verifyEvents(mockServer, "pysparkWriteIcebergTableVersionEnd.json");
  }

  @Test
  void testCreateTable() {
    spark.sql("CREATE TABLE create_table_test (a string, b string) USING iceberg");

    verifyEvents(
        mockServer,
        "pysparkV2CreateTableStartEvent.json",
        "pysparkV2CreateTableCompleteEvent.json");
  }

  @Test
  void testCreateTableAsSelect() {
    clearTables("temp", "source1", "source2", "target");
    createTempDataset().createOrReplaceTempView("temp");

    spark.sql("CREATE TABLE source1 USING iceberg AS SELECT * FROM temp");
    spark.sql("CREATE TABLE source2 USING iceberg AS SELECT * FROM temp");
    spark.sql(
        "CREATE TABLE target USING iceberg AS (SELECT * FROM source1 UNION SELECT * FROM source2)");

    verifyEvents(
        mockServer,
        "pysparkV2CreateTableAsSelectStartEvent.json",
        "pysparkV2CreateTableAsSelectCompleteEvent.json");
  }

  @Test
  void testOverwriteByExpression() {
    clearTables("tbl", "temp");
    createTempDataset().createOrReplaceTempView("temp");

    spark.sql("CREATE TABLE tbl USING iceberg AS SELECT * FROM temp");
    spark.sql("INSERT OVERWRITE tbl VALUES (5,6),(7,8)");

    verifyEvents(
        mockServer,
        "pysparkV2OverwriteByExpressionStartEvent.json",
        "pysparkV2OverwriteByExpressionCompleteEvent.json");
  }

  @Test
  void testOverwriteByPartition() {
    clearTables("tbl", "temp", "source");
    Dataset<Row> dataset =
        spark
            .createDataFrame(
                ImmutableList.of(RowFactory.create(1L, 2L, 3L), RowFactory.create(4L, 5L, 6L)),
                new StructType(
                    new StructField[] {
                      new StructField("a", LongType$.MODULE$, false, Metadata.empty()),
                      new StructField("b", LongType$.MODULE$, false, Metadata.empty()),
                      new StructField("c", LongType$.MODULE$, false, Metadata.empty())
                    }))
            .repartition(1);
    dataset.createOrReplaceTempView("temp");

    spark.sql("CREATE TABLE source USING iceberg AS SELECT * FROM temp");
    spark.sql(
        "CREATE TABLE partitioned_tbl (a long, b  long) USING iceberg PARTITIONED BY (c long)");
    spark.sql("INSERT INTO partitioned_tbl PARTITION (c=1) VALUES (2, 3)");
    spark.sql("INSERT OVERWRITE TABLE partitioned_tbl PARTITION(c) SELECT * FROM source");

    verifyEvents(
        mockServer,
        "pysparkV2OverwritePartitionsStartEvent.json",
        "pysparkV2OverwritePartitionsCompleteEvent.json");
  }

  @Test
  void testReplaceTable() {
    clearTables("tbl_replace", "temp");
    createTempDataset().createOrReplaceTempView("temp");

    spark.sql("CREATE TABLE tbl_replace USING iceberg");
    spark.sql("REPLACE TABLE tbl_replace USING iceberg AS SELECT * FROM temp");

    verifyEvents(
        mockServer,
        "pysparkV2ReplaceTableAsSelectStartEvent.json",
        "pysparkV2ReplaceTableAsSelectCompleteEvent.json");
  }

  @Test
  void testDelete() {
    clearTables("tbl_delete", "temp");
    createTempDataset().createOrReplaceTempView("temp");

    spark.sql("CREATE TABLE tbl_delete USING iceberg AS SELECT * FROM temp");
    spark.sql("DELETE FROM tbl_delete WHERE a=1");

    verifyEvents(mockServer, "pysparkV2DeleteStartEvent.json", "pysparkV2DeleteCompleteEvent.json");
  }

  @Test
  void testUpdate() {
    clearTables("tbl_update", "temp");
    createTempDataset().createOrReplaceTempView("temp");

    spark.sql("CREATE TABLE tbl_update USING iceberg AS SELECT * FROM temp");
    spark.sql("UPDATE tbl_update SET b=5 WHERE a=1");

    verifyEvents(mockServer, "pysparkV2UpdateStartEvent.json", "pysparkV2UpdateCompleteEvent.json");
  }

  @Test
  void testMergeInto() {
    clearTables("events", "updates");
    spark.sql("CREATE TABLE events (event_id long, last_updated_at long) USING iceberg");
    spark.sql("CREATE TABLE updates (event_id long, updated_at long) USING iceberg");

    spark.sql("INSERT INTO events VALUES (1, 1641290276);");
    spark.sql("INSERT INTO updates VALUES (1, 1641290277);");
    spark.sql("INSERT INTO updates VALUES (2, 1641290277);");

    spark.sql(
        "MERGE INTO events USING updates "
            + " ON events.event_id = updates.event_id"
            + " WHEN MATCHED THEN UPDATE SET events.last_updated_at = updates.updated_at"
            + " WHEN NOT MATCHED THEN INSERT (event_id, last_updated_at) "
            + "VALUES (event_id, updated_at)");

    verifyEvents(
        mockServer,
        "pysparkV2MergeIntoTableStartEvent.json",
        "pysparkV2MergeIntoTableCompleteEvent.json");
  }

  @Test
  void testDrop() throws InterruptedException {
    if (System.getProperty("spark.version").matches("3.4.*")) {
      // for Spark 3.4 & Iceberg - dropping directly after creation is not working
      return;
    }
    String tableName = "iceberg_drop_table_test";
    clearTables(tableName);
    spark.sql(String.format("CREATE TABLE %s (a string, b string) USING iceberg", tableName));
    spark.sql(String.format("INSERT INTO %s VALUES ('a', 'b')", tableName));
    Thread.sleep(1000);
    spark.sql("DROP TABLE " + tableName);

    verifyEvents(
        mockServer, "pysparkV2DropTableStartEvent.json", "pysparkV2DropTableCompleteEvent.json");
  }

  @Test
  void testAppend() {
    clearTables("append_source1", "append_source2", "append_table");
    createTempDataset().createOrReplaceTempView("temp");

    spark.sql("CREATE TABLE append_source1 USING iceberg AS SELECT a FROM temp");
    spark.sql("CREATE TABLE append_source2 USING iceberg AS SELECT a FROM temp");

    spark.sql("CREATE TABLE append_table (a long) USING iceberg");
    spark.sql(
        "INSERT INTO append_table "
            + "(SELECT * FROM append_source1 UNION SELECT * FROM append_source2);");

    verifyEvents(
        mockServer, "pysparkV2AppendDataStartEvent.json", "pysparkV2AppendDataCompleteEvent.json");
  }

  private Dataset<Row> createTempDataset() {
    return spark
        .createDataFrame(
            ImmutableList.of(RowFactory.create(1L, 2L), RowFactory.create(3L, 4L)),
            new StructType(
                new StructField[] {
                  new StructField("a", LongType$.MODULE$, false, Metadata.empty()),
                  new StructField("b", LongType$.MODULE$, false, Metadata.empty())
                }))
        .repartition(1);
  }

  private void clearTables(String... tables) {
    Arrays.asList(tables).stream()
        .filter(t -> spark.catalog().tableExists(t))
        .forEach(t -> spark.sql("DROP TABLE IF EXISTS " + t));
  }
}
