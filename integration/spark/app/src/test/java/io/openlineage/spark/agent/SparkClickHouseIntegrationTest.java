/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent;

import static io.openlineage.spark.agent.SparkTestUtils.createHttpServer;
import static org.assertj.core.api.Assertions.assertThat;

import com.sun.net.httpserver.HttpServer;
import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.SparkTestUtils.OpenLineageEndpointHandler;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.clickhouse.ClickHouseContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Verifies lineage extraction for reads through the official ClickHouse Spark connector
 * (com.clickhouse.spark.ClickHouseCatalog) against a real ClickHouse server. The connector is added
 * to the test classpath by {@code clickhouseDependencies} in app/build.gradle; cells of the test
 * matrix without a connector artifact exclude tests tagged `clickhouse`.
 */
@Tag("integration-test")
@Tag("clickhouse")
class SparkClickHouseIntegrationTest {
  private static final int CLICKHOUSE_HTTP_PORT = 8123;
  private static final String CLICKHOUSE = "clickhouse";
  private static final String CLICKHOUSE_NAMESPACE_PREFIX = "clickhouse://localhost:";
  private static final String TABLE_NAME = "mydb.people";
  private static final String QUALIFIED_TABLE_NAME = CLICKHOUSE + "." + TABLE_NAME;

  private final OpenLineageEndpointHandler handler = new OpenLineageEndpointHandler();
  private HttpServer server;
  private ClickHouseContainer clickhouse;
  private SparkSession spark;

  @BeforeEach
  void beforeEach() throws IOException, InterruptedException {
    server = createHttpServer(handler);
    clickhouse =
        new ClickHouseContainer(DockerImageName.parse("clickhouse/clickhouse-server:24.8"));
    clickhouse.start();

    exec("CREATE DATABASE IF NOT EXISTS mydb");
    exec("CREATE TABLE " + TABLE_NAME + " (id Int32, name String) ENGINE = MergeTree ORDER BY id");
    exec("INSERT INTO " + TABLE_NAME + " VALUES (1, 'John'), (2, 'Jane')");
  }

  @AfterEach
  void afterEach() {
    try {
      stopSpark();
    } finally {
      try {
        if (clickhouse != null) {
          clickhouse.stop();
        }
      } finally {
        if (server != null) {
          server.stop(0);
        }
      }
    }
  }

  @Test
  void testClickHouseCatalogDatasetIdentifierWhenTableIsRead() {
    int httpPort = clickhouse.getMappedPort(CLICKHOUSE_HTTP_PORT);

    spark = createSparkSession("testClickHouseCatalogDatasetIdentifierWhenTableIsRead");

    spark.sql("SELECT * FROM " + QUALIFIED_TABLE_NAME).show();
    stopSpark();

    List<OpenLineage.InputDataset> inputs =
        readInputs("test_click_house_catalog_dataset_identifier_when_table_is_read");

    assertThat(inputs).isNotEmpty();
    inputs.forEach(
        input -> {
          assertThat(input.getNamespace()).isEqualTo(CLICKHOUSE_NAMESPACE_PREFIX + httpPort);
          assertThat(input.getName()).isEqualTo(TABLE_NAME);
        });
  }

  @Test
  void testClickHouseCatalogDatasetFacetsWhenTableIsRead() {
    spark = createSparkSession("testClickHouseCatalogDatasetFacetsWhenTableIsRead");

    spark.sql("SELECT * FROM " + QUALIFIED_TABLE_NAME).show();
    stopSpark();

    List<OpenLineage.InputDataset> inputs =
        readInputs("test_click_house_catalog_dataset_facets_when_table_is_read");

    assertThat(inputs).isNotEmpty();

    List<OpenLineage.StorageDatasetFacet> storageFacets =
        inputs.stream()
            .map(OpenLineage.Dataset::getFacets)
            .map(OpenLineage.DatasetFacets::getStorage)
            .filter(Objects::nonNull)
            .collect(Collectors.toList());
    assertThat(storageFacets).isNotEmpty();
    assertThat(storageFacets.get(0).getStorageLayer()).isEqualTo(CLICKHOUSE);
    assertThat(storageFacets.get(0).getFileFormat()).isEqualTo("MergeTree");

    List<OpenLineage.CatalogDatasetFacet> catalogFacets =
        inputs.stream()
            .map(OpenLineage.Dataset::getFacets)
            .map(OpenLineage.DatasetFacets::getCatalog)
            .filter(Objects::nonNull)
            .collect(Collectors.toList());
    assertThat(catalogFacets).isNotEmpty();
    assertThat(catalogFacets.get(0).getName()).isEqualTo(CLICKHOUSE);
    assertThat(catalogFacets.get(0).getFramework()).isEqualTo(CLICKHOUSE);
    assertThat(catalogFacets.get(0).getType()).isEqualTo(CLICKHOUSE);
    assertThat(catalogFacets.get(0).getSource()).isEqualTo("spark");
  }

  @Test
  void testClickHouseCatalogDatasetIdentifierWhenTableIsWritten() {
    int httpPort = clickhouse.getMappedPort(CLICKHOUSE_HTTP_PORT);
    spark = createSparkSession("testClickHouseCatalogDatasetIdentifierWhenTableIsWritten");

    spark.sql(
        "INSERT INTO " + QUALIFIED_TABLE_NAME + " SELECT CAST(3 AS INT) AS id, 'Ada' AS name");
    stopSpark();

    List<OpenLineage.OutputDataset> outputs =
        readOutputs("test_click_house_catalog_dataset_identifier_when_table_is_written");

    assertThat(outputs).isNotEmpty();
    outputs.forEach(
        output -> {
          assertThat(output.getNamespace()).isEqualTo(CLICKHOUSE_NAMESPACE_PREFIX + httpPort);
          assertThat(output.getName()).isEqualTo(TABLE_NAME);
          OpenLineage.CatalogDatasetFacet catalogFacet = output.getFacets().getCatalog();
          assertThat(catalogFacet).isNotNull();
          assertThat(catalogFacet.getName()).isEqualTo(CLICKHOUSE);
          assertThat(catalogFacet.getFramework()).isEqualTo(CLICKHOUSE);
          assertThat(catalogFacet.getType()).isEqualTo(CLICKHOUSE);
          assertThat(catalogFacet.getSource()).isEqualTo("spark");
        });
  }

  private List<OpenLineage.InputDataset> readInputs(String jobName) {
    return handler.getEvents(jobName).stream()
        .filter(event -> !event.getInputs().isEmpty())
        .flatMap(event -> event.getInputs().stream())
        .collect(Collectors.toList());
  }

  private List<OpenLineage.OutputDataset> readOutputs(String jobName) {
    return handler.getEvents(jobName).stream()
        .filter(event -> !event.getOutputs().isEmpty())
        .flatMap(event -> event.getOutputs().stream())
        .collect(Collectors.toList());
  }

  private void exec(String sql) throws IOException, InterruptedException {
    clickhouse.execInContainer("clickhouse-client", "--query", sql);
  }

  private void stopSpark() {
    if (spark != null && !spark.sparkContext().isStopped()) {
      spark.stop();
    }
  }

  private SparkSession createSparkSession(String appName) {
    Path tmpDir = Paths.get(System.getProperty("java.io.tmpdir"));
    String testId = TestIds.randomHex();
    Path derbySystemHome = tmpDir.resolve("derby").resolve(testId);
    Path sparkSqlWarehouse = tmpDir.resolve("spark-sql-warehouse").resolve(testId);

    return SparkSession.builder()
        .appName(appName)
        .master("local[*]")
        .config("spark.extraListeners", OpenLineageSparkListener.class.getCanonicalName())
        .config("spark.driver.host", "localhost")
        .config("spark.driver.extraJavaOptions", "-Dderby.system.home=" + derbySystemHome)
        .config("spark.sql.warehouse.dir", sparkSqlWarehouse.toString())
        .config("spark.ui.enabled", false)
        .config("spark.openlineage.transport.type", "http")
        .config(
            "spark.openlineage.transport.url", "http://localhost:" + server.getAddress().getPort())
        .config("spark.openlineage.facets.spark_unknown.disabled", "true")
        .config("spark.clickhouse.write.format", "json")
        .config("spark.sql.catalog.clickhouse", "com.clickhouse.spark.ClickHouseCatalog")
        .config("spark.sql.catalog.clickhouse.host", clickhouse.getHost())
        .config("spark.sql.catalog.clickhouse.protocol", "http")
        .config(
            "spark.sql.catalog.clickhouse.http_port",
            String.valueOf(clickhouse.getMappedPort(CLICKHOUSE_HTTP_PORT)))
        .config("spark.sql.catalog.clickhouse.user", clickhouse.getUsername())
        .config("spark.sql.catalog.clickhouse.password", clickhouse.getPassword())
        .getOrCreate();
  }
}
