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
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.clickhouse.ClickHouseContainer;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

/**
 * Verifies lineage extraction for reads through the official ClickHouse Spark connector
 * (com.clickhouse.spark.ClickHouseCatalog) against a real ClickHouse server. The connector is added
 * to the test classpath by {@code clickhouseDependencies} in app/build.gradle; cells of the test
 * matrix without a connector artifact exclude tests tagged `clickhouse`.
 */
@Tag("integration-test")
@Tag("clickhouse")
@Testcontainers
@Slf4j
class SparkClickHouseIntegrationTest {
  private static final OpenLineageEndpointHandler handler = new OpenLineageEndpointHandler();
  private static final int CLICKHOUSE_HTTP_PORT = 8123;

  @Test
  void testClickHouseCatalogDatasetIdentifierWhenTableIsRead()
      throws IOException, InterruptedException {
    HttpServer server = createHttpServer(handler);
    ClickHouseContainer clickhouse = startClickHouseContainer();
    int httpPort = clickhouse.getMappedPort(CLICKHOUSE_HTTP_PORT);

    SparkSession spark =
        createSparkSession(
            server.getAddress().getPort(),
            clickhouse,
            "testClickHouseCatalogDatasetIdentifierWhenTableIsRead");

    spark.sql("SELECT * FROM clickhouse.mydb.people").show();

    clickhouse.stop();
    spark.stop();

    List<OpenLineage.InputDataset> inputs =
        readInputs("test_click_house_catalog_dataset_identifier_when_table_is_read");

    assertThat(inputs).isNotEmpty();
    inputs.forEach(
        input -> {
          assertThat(input.getNamespace()).isEqualTo("clickhouse://localhost:" + httpPort);
          assertThat(input.getName()).isEqualTo("mydb.people");
        });
  }

  @Test
  void testClickHouseCatalogDatasetFacetsWhenTableIsRead()
      throws IOException, InterruptedException {
    HttpServer server = createHttpServer(handler);
    ClickHouseContainer clickhouse = startClickHouseContainer();

    SparkSession spark =
        createSparkSession(
            server.getAddress().getPort(),
            clickhouse,
            "testClickHouseCatalogDatasetFacetsWhenTableIsRead");

    spark.sql("SELECT * FROM clickhouse.mydb.people").show();

    clickhouse.stop();
    spark.stop();

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
    assertThat(storageFacets.get(0).getStorageLayer()).isEqualTo("clickhouse");
    assertThat(storageFacets.get(0).getFileFormat()).isEqualTo("MergeTree");

    List<OpenLineage.CatalogDatasetFacet> catalogFacets =
        inputs.stream()
            .map(OpenLineage.Dataset::getFacets)
            .map(OpenLineage.DatasetFacets::getCatalog)
            .filter(Objects::nonNull)
            .collect(Collectors.toList());
    assertThat(catalogFacets).isNotEmpty();
    assertThat(catalogFacets.get(0).getName()).isEqualTo("clickhouse");
    assertThat(catalogFacets.get(0).getFramework()).isEqualTo("clickhouse");
    assertThat(catalogFacets.get(0).getType()).isEqualTo("clickhouse");
    assertThat(catalogFacets.get(0).getSource()).isEqualTo("spark");
  }

  private List<OpenLineage.InputDataset> readInputs(String jobName) {
    return handler.getEvents(jobName).stream()
        .filter(event -> !event.getInputs().isEmpty())
        .flatMap(event -> event.getInputs().stream())
        .collect(Collectors.toList());
  }

  private ClickHouseContainer startClickHouseContainer() throws IOException, InterruptedException {
    ClickHouseContainer clickhouse =
        new ClickHouseContainer(DockerImageName.parse("clickhouse/clickhouse-server:24.8"));
    clickhouse.start();

    exec(clickhouse, "CREATE DATABASE IF NOT EXISTS mydb");
    exec(
        clickhouse,
        "CREATE TABLE mydb.people (id Int32, name String) ENGINE = MergeTree ORDER BY id");
    exec(clickhouse, "INSERT INTO mydb.people VALUES (1, 'John'), (2, 'Jane')");
    return clickhouse;
  }

  private void exec(ClickHouseContainer clickhouse, String sql)
      throws IOException, InterruptedException {
    clickhouse.execInContainer("clickhouse-client", "--query", sql);
  }

  private SparkSession createSparkSession(
      Integer httpServerPort, ClickHouseContainer clickhouse, String appName) {
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
        .config("spark.openlineage.transport.url", "http://localhost:" + httpServerPort)
        .config("spark.openlineage.facets.spark_unknown.disabled", "true")
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
