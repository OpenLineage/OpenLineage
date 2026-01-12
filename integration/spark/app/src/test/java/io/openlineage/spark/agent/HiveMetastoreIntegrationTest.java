/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent;

import static io.openlineage.spark.agent.MockServerUtils.getEventsEmitted;
import static io.openlineage.spark.agent.MockServerUtils.verifyEvents;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;

import io.openlineage.client.OpenLineage;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.LongType$;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.mockserver.integration.ClientAndServer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Slf4j
@Tag("integration-test")
@Testcontainers
@EnabledIfSystemProperty(named = "spark.version", matches = "([34].*)")
class HiveMetastoreIntegrationTest {

  private static final int MOCKSERVER_PORT = 2000;
  private static final Network network = Network.newNetwork();
  private static SparkSession spark;
  private static ClientAndServer mockServer;

  @Container
  private static final GenericContainer<?> metastoreContainer =
      SparkContainerUtils.makeHiveMetastoreContainer(network);

  @BeforeAll
  public static void setup() throws IOException {
    mockServer = MockServerUtils.createAndConfigureMockServer(MOCKSERVER_PORT);
  }

  @AfterAll
  public static void afterAll() {
    MockServerUtils.stopMockServer(mockServer);
  }

  @BeforeEach
  void beforeEach() throws IOException {
    FileUtils.deleteDirectory(new File("/tmp/hive-metastore/"));
    MockServerUtils.clearRequests(mockServer);
    spark =
        SparkSession.builder()
            .master("local[*]")
            .appName("HiveMetastoreIntegrationTest")
            .config("spark.sql.shuffle.partitions", 1)
            .config("spark.sql.catalogImplementation", "hive")
            .config("spark.sql.warehouse.dir", "file:/tmp/hive-metastore/")
            .config(
                "hive.metastore.uris",
                "thrift://localhost:" + metastoreContainer.getMappedPort(9083))
            .config("spark.extraListeners", OpenLineageSparkListener.class.getName())
            .config("spark.openlineage.transport.type", "http")
            .config(
                "spark.openlineage.transport.url",
                "http://localhost:" + MOCKSERVER_PORT + "/api/v1/lineage")
            .enableHiveSupport()
            .getOrCreate();
  }

  @Test
  void testCatalogFacets() {
    spark.sql("create database if not exists hive3");
    createTempDataset(2).createOrReplaceTempView("temp");
    spark.sql("CREATE TABLE IF NOT EXISTS hive3.source1 using hive AS SELECT * FROM temp");
    spark.sql("CREATE TABLE IF NOT EXISTS hive3.source2 using hive AS SELECT * FROM temp");
    spark.sql(
        "CREATE TABLE hive3.target using hive AS (SELECT * FROM hive3.source1 UNION SELECT * FROM hive3.source2)");
    List<OpenLineage.RunEvent> events = getEventsEmitted(mockServer);

    String expectedMetadataUri = "hive://localhost:" + metastoreContainer.getMappedPort(9083);
    String expectedWarehouseUri = "file:/tmp/hive-metastore";

    assertThat(events)
        .flatExtracting(OpenLineage.RunEvent::getInputs)
        .filteredOn(input -> input.getName().contains("hive3."))
        .allMatch(
            input -> {
              OpenLineage.CatalogDatasetFacet catalogFacet = input.getFacets().getCatalog();
              return catalogFacet != null
                  && "default".equals(catalogFacet.getName())
                  && "hive".equals(catalogFacet.getFramework())
                  && "hive".equals(catalogFacet.getType())
                  && "spark".equals(catalogFacet.getSource())
                  && expectedMetadataUri.equals(catalogFacet.getMetadataUri())
                  && expectedWarehouseUri.equals(catalogFacet.getWarehouseUri());
            });

    assertThat(events)
        .flatExtracting(OpenLineage.RunEvent::getOutputs)
        .filteredOn(output -> output.getName().contains("hive3."))
        .allMatch(
            output -> {
              OpenLineage.CatalogDatasetFacet catalogFacet = output.getFacets().getCatalog();
              return catalogFacet != null
                  && "default".equals(catalogFacet.getName())
                  && "hive".equals(catalogFacet.getFramework())
                  && "hive".equals(catalogFacet.getType())
                  && "spark".equals(catalogFacet.getSource())
                  && expectedMetadataUri.equals(catalogFacet.getMetadataUri())
                  && expectedWarehouseUri.equals(catalogFacet.getWarehouseUri());
            });
  }

  @Test
  void testSaveAsTable() {
    spark.sql("create database if not exists hive3");

    Dataset<Row> df =
        spark.createDataFrame(
            singletonList(RowFactory.create("New York")),
            new StructType().add("city", DataTypes.StringType, true));
    df.write().mode("overwrite").saveAsTable("hive3.table1");

    spark.read().table("hive3.table1").write().mode("overwrite").saveAsTable("hive3.table2");

    verifyEvents(
        mockServer,
        "hiveCreateDataSourceAsSelectCommandStartEvent.json",
        "hiveCreateDataSourceAsSelectCommandCompleteEvent.json");
  }

  @Test
  void testCreateHiveTableAsSelect() {
    spark.sql("create database if not exists hive3");

    Dataset<Row> df =
        spark.createDataFrame(
            singletonList(RowFactory.create("New York")),
            new StructType().add("city", DataTypes.StringType, true));
    df.write().mode("overwrite").saveAsTable("hive3.table1");

    spark.sql("CREATE TABLE IF NOT EXISTS hive3.table2 using hive AS SELECT * FROM hive3.table1");

    verifyEvents(
        mockServer,
        "hiveCreateHiveTableAsSelectCommandStartEvent.json",
        "hiveCreateHiveTableAsSelectCommandCompleteEvent.json");
  }

  private Dataset<Row> createTempDataset(int rows) {
    List<Row> rowList =
        Arrays.stream(IntStream.rangeClosed(1, rows).toArray())
            .mapToObj(i -> RowFactory.create((long) i, (long) i + 1))
            .collect(Collectors.toList());

    return spark
        .createDataFrame(
            rowList,
            new StructType(
                new StructField[] {
                  new StructField("a", LongType$.MODULE$, false, Metadata.empty()),
                  new StructField("b", LongType$.MODULE$, false, Metadata.empty())
                }))
        .repartition(1);
  }
}
