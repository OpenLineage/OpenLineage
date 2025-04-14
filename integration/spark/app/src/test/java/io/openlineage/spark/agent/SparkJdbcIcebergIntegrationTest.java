/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent;

import static io.openlineage.spark.agent.MockServerUtils.getEventsEmittedWithJobName;
import static io.openlineage.spark.agent.SparkTestUtils.SPARK_VERSION;
import static org.assertj.core.api.Assertions.assertThat;

import io.openlineage.client.OpenLineage;
import java.util.Arrays;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
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
import org.junit.jupiter.api.condition.DisabledIf;
import org.mockserver.integration.ClientAndServer;

@Tag("integration-test")
@Tag("iceberg")
@Slf4j
class SparkJdbcIcebergIntegrationTest {

  private static final int MOCK_SERVER_PORT = 1084;

  @SuppressWarnings("PMD")
  private static final String LOCAL_IP = "127.0.0.1";

  private static final String ICEBERG_PATH = "/tmp/jdbciceberg";

  private static ClientAndServer mockServer;
  private static SparkSession spark;

  @BeforeAll
  @SneakyThrows
  public static void beforeAll() {
    SparkSession$.MODULE$.cleanupAnyExistingSession();
    //    FileUtils.deleteDirectory(new File(ICEBERG_PATH));
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
            .config("spark.sql.warehouse.dir", "file:" + ICEBERG_PATH)
            .config("spark.driver.extraJavaOptions", "-Dderby.system.home=" + ICEBERG_PATH)
            .config("spark.openlineage.transport.type", "http")
            .config(
                "spark.openlineage.transport.url",
                "http://localhost:" + mockServer.getPort() + "/api/v1/namespaces/iceberg-namespace")
            .config("spark.openlineage.facets.debug.disabled", "false")
            .config("spark.extraListeners", OpenLineageSparkListener.class.getName())
            .config("spark.sql.defaultCatalog", "jdbc")
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config("spark.sql.catalog.jdbc.type", "jdbc")
            .config("spark.sql.catalog.jdbc", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.jdbc.uri", "jdbc:sqlite:/tmp/iceberg_catalog.sqlite")
            .config("spark.sql.catalog.jdbc.warehouse", ICEBERG_PATH)
            .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
            .config(
                "spark.openlineage.dataset.removePath.pattern",
                "(.*)(?<remove>\\_666)") // removes _666 from dataset name
            .getOrCreate();
  }

  @Test
  @DisabledIf("isJava8AndSpark35")
  @SuppressWarnings("PMD.JUnitTestsShouldIncludeAssert")
  void testJdbcCatalog() {
    createTempDataset(3).createOrReplaceTempView("temp");
    spark.sql(
        "CREATE TABLE IF NOT EXISTS jdbc.default.scan_source USING iceberg AS SELECT * FROM temp");
    spark.sql(
        "CREATE TABLE  IF NOT EXISTS jdbc.default.scan_target USING iceberg AS SELECT * FROM jdbc.default.scan_source");

    List<OpenLineage.RunEvent> runEvents = getEventsEmittedWithJobName(mockServer, "scan_target");

    assertThat(
            runEvents.stream()
                .flatMap(e -> e.getInputs().stream())
                .filter(
                    e ->
                        hasSymlinkWith(
                            e,
                            symlink ->
                                symlink.getName().equals("default.scan_source")
                                    && symlink.getNamespace().equals("sqlite:/tmp/iceberg_catalog.sqlite"))))
        .isNotEmpty();

    assertThat(
            runEvents.stream()
                .flatMap(e -> e.getOutputs().stream())
                .filter(
                    e ->
                        hasSymlinkWith(
                            e,
                            symlink ->
                                symlink.getName().endsWith("default.scan_target")
                                    && symlink.getNamespace().equals("sqlite:/tmp/iceberg_catalog.sqlite"))))
        .isNotEmpty();
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

  private boolean hasSymlinkWith(
      OpenLineage.Dataset dataset,
      Predicate<OpenLineage.SymlinksDatasetFacetIdentifiers> predicate) {
    return dataset.getFacets().getSymlinks().getIdentifiers().stream().anyMatch(predicate);
  }

  @SuppressWarnings("PMD.UnusedPrivateMethod")
  private static boolean isJava8AndSpark35() {
    return System.getProperty("java.version").startsWith("1.8")
        && System.getProperty(SPARK_VERSION).startsWith("3.5");
  }
}
