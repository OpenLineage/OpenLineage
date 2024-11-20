/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent;

import static io.openlineage.spark.agent.MockServerUtils.getEventsEmittedWithJobName;
import static io.openlineage.spark.agent.MockServerUtils.verifyEvents;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockserver.model.HttpRequest.request;

import com.google.common.collect.ImmutableList;
import io.openlineage.client.OpenLineage.ColumnLineageDatasetFacet;
import io.openlineage.client.OpenLineage.RunEvent;
import io.openlineage.client.OpenLineage.RunFacet;
import io.openlineage.client.OpenLineageClientUtils;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
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
import org.mockserver.integration.ClientAndServer;
import org.mockserver.model.HttpRequest;

@Tag("integration-test")
@Tag("iceberg")
@Slf4j
class SparkIcebergIntegrationTest {
  private static final int MOCK_SERVER_PORT = 1084;

  @SuppressWarnings("PMD")
  private static final String LOCAL_IP = "127.0.0.1";

  private static ClientAndServer mockServer;
  private static SparkSession spark;

  @BeforeAll
  @SneakyThrows
  public static void beforeAll() {
    SparkSession$.MODULE$.cleanupAnyExistingSession();
    FileUtils.deleteDirectory(new File("/tmp/iceberg/"));
    mockServer = MockServerUtils.createAndConfigureMockServer(MOCK_SERVER_PORT);
  }

  @AfterAll
  @SneakyThrows
  public static void afterAll() {
    SparkSession$.MODULE$.cleanupAnyExistingSession();
    MockServerUtils.stopMockServer(mockServer);
  }

  @BeforeEach
  @SneakyThrows
  public void beforeEach() {
    MockServerUtils.clearRequests(mockServer);
    spark =
        SparkSession.builder()
            .master("local[*]")
            .appName("IcebergIntegrationTest")
            .config("spark.driver.host", LOCAL_IP)
            .config("spark.driver.bindAddress", LOCAL_IP)
            .config("spark.ui.enabled", false)
            .config("spark.sql.shuffle.partitions", 1)
            .config("spark.sql.warehouse.dir", "file:/tmp/iceberg/")
            .config("spark.driver.extraJavaOptions", "-Dderby.system.home=/tmp/iceberg")
            .config("spark.openlineage.transport.type", "http")
            .config(
                "spark.openlineage.transport.url",
                "http://localhost:" + mockServer.getPort() + "/api/v1/namespaces/iceberg-namespace")
            .config("spark.openlineage.facets.debug.disabled", "false")
            .config("spark.extraListeners", OpenLineageSparkListener.class.getName())
            .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.spark_catalog.type", "hadoop")
            .config("spark.sql.catalog.spark_catalog.warehouse", "/tmp/iceberg")
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
            .config(
                "spark.openlineage.dataset.removePath.pattern",
                "(.*)(?<remove>\\_666)") // removes _666 from dataset name
            .getOrCreate();
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

    List<RunEvent> events = MockServerUtils.getEventsEmitted(mockServer);
    Stream<ColumnLineageDatasetFacet> datasetFacetStream =
        events.stream()
            .filter(e -> !e.getOutputs().isEmpty())
            .map(e -> e.getOutputs().get(0))
            .filter(d -> d.getName().contains("target")) // get all event target dataset events
            .filter(d -> d.getFacets().getColumnLineage() != null)
            .map(d -> d.getFacets().getColumnLineage());

    // assert that each field has exactly two input fields within all the facets published
    datasetFacetStream.forEach(
        cf -> {
          assertThat(
                  cf.getFields().getAdditionalProperties().get("a").getInputFields().stream()
                      .map(f -> f.getField())
                      .collect(Collectors.toList()))
              .hasSize(2)
              .containsExactly("a", "a");

          assertThat(
                  cf.getFields().getAdditionalProperties().get("b").getInputFields().stream()
                      .map(f -> f.getField())
                      .collect(Collectors.toList()))
              .hasSize(2)
              .containsExactly("b", "b");
        });
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

  @Test
  void testRemovePathPattern() throws InterruptedException {
    clearTables("tbl_remove_path_666", "temp", "input_table_666");
    createTempDataset().createOrReplaceTempView("temp");

    spark.sql("CREATE TABLE input_table_666 USING iceberg AS SELECT * FROM temp");
    spark.sql("CREATE TABLE tbl_remove_path_666 USING iceberg AS SELECT a FROM input_table_666");

    List<RunEvent> jobEvents = getEventsEmittedWithJobName(mockServer, "default_tbl_remove_path");

    assertThat(
            jobEvents.stream()
                .filter(e -> !e.getOutputs().isEmpty())
                .filter(e -> e.getOutputs().get(0).getName().endsWith("tbl_remove_path")))
        .isNotEmpty();

    assertThat(
            jobEvents.stream()
                .filter(e -> !e.getInputs().isEmpty())
                .filter(e -> e.getInputs().get(0).getName().endsWith("input_table")))
        .isNotEmpty();
  }

  @Test
  @SuppressWarnings("PMD.JUnitTestContainsTooManyAsserts")
  void testDebugFacet() {
    clearTables("iceberg_temp", "temp");
    createTempDataset().createOrReplaceTempView("temp");
    spark.sql("CREATE TABLE iceberg_temp USING iceberg AS SELECT * FROM temp");

    HttpRequest[] httpRequests =
        mockServer.retrieveRecordedRequests(request().withPath("/api/v1/lineage"));
    RunEvent event =
        OpenLineageClientUtils.runEventFromJson(
            httpRequests[httpRequests.length - 1].getBodyAsString());

    assertThat(event.getRun().getFacets().getAdditionalProperties()).containsKey("debug");

    RunFacet debug = event.getRun().getFacets().getAdditionalProperties().get("debug");

    // verify classpath
    Map<String, Object> classpathFacet =
        (Map<String, Object>) debug.getAdditionalProperties().get("classpath");
    assertThat(classpathFacet)
        .containsKeys("openLineageVersion", "sparkVersion", "scalaVersion", "classDetails");
    assertThat((ArrayList) classpathFacet.get("classDetails")).hasSize(3);
    assertThat((Map) ((ArrayList) classpathFacet.get("classDetails")).get(1))
        .containsEntry("onClasspath", true)
        .containsEntry("className", "org.apache.iceberg.catalog.Catalog");

    // verify system
    Map<String, Object> systemFacet =
        (Map<String, Object>) debug.getAdditionalProperties().get("system");

    assertThat((String) systemFacet.get("sparkDeployMode")).isEqualTo("client");
    assertThat((String) systemFacet.get("javaVersion")).isNotEmpty();
    assertThat((String) systemFacet.get("javaVendor")).isNotEmpty();
    assertThat((String) systemFacet.get("osArch")).isNotEmpty();
    assertThat((String) systemFacet.get("osName")).isNotEmpty();
    assertThat((String) systemFacet.get("osVersion")).isNotEmpty();
    assertThat((String) systemFacet.get("userLanguage")).isNotEmpty();
    assertThat((String) systemFacet.get("userTimezone")).isNotEmpty();

    // verify logical plan
    Map<String, Object> logicalPlan =
        (Map<String, Object>) debug.getAdditionalProperties().get("logicalPlan");
    ArrayList<HashMap<String, Object>> nodes =
        (ArrayList<HashMap<String, Object>>) logicalPlan.get("nodes");

    assertThat(nodes.size()).isGreaterThan(0); // LogicalPlan differs per Spark Version
    assertThat(((String) nodes.get(0).get("id")).substring(0, 6)).containsAnyOf("Append", "Create");
    assertThat(((String) nodes.get(0).get("desc")).substring(0, 6))
        .containsAnyOf("Append", "Create");

    // verify spark config
    Map<String, Object> configFacet =
        (Map<String, Object>) debug.getAdditionalProperties().get("config");

    assertThat(configFacet)
        .containsEntry("extraListeners", "io.openlineage.spark.agent.OpenLineageSparkListener")
        .containsEntry("catalogClass", "org.apache.spark.sql.internal.CatalogImpl")
        .containsEntry(
            "extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions");

    assertThat((Map<String, String>) configFacet.get("openLineageConfig"))
        .containsKeys("namespace");
    assertThat(
            (Map<String, String>)
                ((Map<?, ?>) configFacet.get("openLineageConfig")).get("transport"))
        .containsKeys("type", "url", "endpoint");
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
