/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent;

import static io.openlineage.spark.agent.MockServerUtils.getEventsEmittedWithJobName;
import static io.openlineage.spark.agent.SparkTestUtils.SPARK_VERSION;
import static org.assertj.core.api.Assertions.assertThat;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.InputDataset;
import io.openlineage.client.OpenLineage.InputDatasetInputFacets;
import io.openlineage.client.OpenLineage.OutputDataset;
import io.openlineage.client.OpenLineage.OutputDatasetOutputFacets;
import io.openlineage.client.OpenLineage.RunEvent;
import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
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
import org.assertj.core.api.AbstractObjectAssert;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIf;
import org.mockserver.integration.ClientAndServer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.junit.jupiter.Testcontainers;

@Tag("integration-test")
@Tag("iceberg")
@Testcontainers
@Slf4j
class SparkRestIcebergIntegrationTest {
  private static final int MOCK_SERVER_PORT = 1084;

  @SuppressWarnings("PMD")
  private static final String LOCAL_IP = "127.0.0.1";

  private static ClientAndServer mockServer;
  private static SparkSession spark;
  private static final Network network = Network.newNetwork();
  private static GenericContainer<?> icebergRestContainer =
      new GenericContainer<>("tabulario/iceberg-rest:latest")
          .withExposedPorts(8181)
          .withNetwork(network);

  @BeforeAll
  @SneakyThrows
  public static void beforeAll() {
    SparkSession$.MODULE$.cleanupAnyExistingSession();
    FileUtils.deleteDirectory(new File("/tmp/iceberg/"));
    mockServer = MockServerUtils.createAndConfigureMockServer(MOCK_SERVER_PORT);
    icebergRestContainer.start();
  }

  @AfterAll
  @SneakyThrows
  public static void afterAll() {
    SparkSession$.MODULE$.cleanupAnyExistingSession();
    MockServerUtils.stopMockServer(mockServer);
    icebergRestContainer.stop();
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
            .config("spark.driver.extraJavaOptions", "-Dderby.system.home=/tmp/iceberg")
            .config("spark.openlineage.transport.type", "http")
            .config(
                "spark.openlineage.transport.url",
                "http://localhost:" + mockServer.getPort() + "/api/v1/namespaces/iceberg-namespace")
            .config("spark.extraListeners", OpenLineageSparkListener.class.getName())
            .config("spark.sql.catalog.rest", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.rest.catalog-impl", "org.apache.iceberg.rest.RESTCatalog")
            .config(
                "spark.sql.catalog.rest.uri",
                "http://"
                    + icebergRestContainer.getHost()
                    + ":"
                    + icebergRestContainer.getMappedPort(8181))
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .getOrCreate();

    clearTables("temp", "scan_source", "scan_target");
    createTempDataset(3).createOrReplaceTempView("temp");
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

  @Test
  @DisabledIf("isJava8AndSpark35")
  @SuppressWarnings("PMD.JUnitTestsShouldIncludeAssert")
  void testMetricsFacets() {
    spark.sql("CREATE TABLE rest.default.scan_source USING iceberg AS SELECT * FROM temp");
    spark.sql(
        "CREATE TABLE rest.default.scan_target USING iceberg AS SELECT * FROM rest.default.scan_source");

    List<RunEvent> runEvents = getEventsEmittedWithJobName(mockServer, "scan_target");

    List<InputDatasetInputFacets> inputFacets =
        runEvents.stream()
            .flatMap(e -> e.getInputs().stream())
            .filter(e -> e.getName().endsWith("scan_source"))
            .map(InputDataset::getInputFacets)
            .filter(Objects::nonNull)
            .collect(Collectors.toList());

    // get scan report facet
    AbstractObjectAssert<?, ?> icebergScanReport =
        assertThat(inputFacets.stream())
            .map(l -> l.getAdditionalProperties())
            .filteredOn(Objects::nonNull)
            .filteredOn(e -> e.containsKey("icebergScanReport"))
            .isNotEmpty()
            .map(e -> e.get("icebergScanReport"))
            .map(e -> e.getAdditionalProperties())
            .singleElement();

    icebergScanReport
        .extracting("snapshotId")
        .asInstanceOf(InstanceOfAssertFactories.LONG)
        .isGreaterThan(0);

    // test commit report
    List<OutputDatasetOutputFacets> outputFacets =
        runEvents.stream()
            .flatMap(e -> e.getOutputs().stream())
            .filter(e -> e.getName().endsWith("scan_target"))
            .map(OutputDataset::getOutputFacets)
            .filter(Objects::nonNull)
            .collect(Collectors.toList());

    // get scan report facet
    AbstractObjectAssert<?, ?> icebergCommitReport =
        assertThat(outputFacets.stream())
            .map(l -> l.getAdditionalProperties())
            .filteredOn(Objects::nonNull)
            .filteredOn(e -> e.containsKey("icebergCommitReport"))
            .isNotEmpty()
            .map(e -> e.get("icebergCommitReport"))
            .map(e -> e.getAdditionalProperties())
            .first();

    icebergCommitReport
        .extracting("snapshotId")
        .asInstanceOf(InstanceOfAssertFactories.LONG)
        .isGreaterThan(0);

    Optional<OpenLineage.CatalogDatasetFacet> catalogDatasetFacet =
        runEvents.stream()
            .flatMap(e -> e.getInputs().stream())
            .filter(e -> e.getName().endsWith("scan_source"))
            .map(dataset -> dataset.getFacets().getCatalog())
            .filter(Objects::nonNull)
            .findFirst();

    assertThat(catalogDatasetFacet).isPresent();
    OpenLineage.CatalogDatasetFacet catalog = catalogDatasetFacet.get();
    assertThat(catalog.getType()).isEqualTo("rest");
    assertThat(catalog.getFramework()).isEqualTo("iceberg");
    assertThat(catalog.getName()).isEqualTo("rest");
    assertThat(catalog.getMetadataUri())
        .isEqualTo(
            "http://"
                + icebergRestContainer.getHost()
                + ":"
                + icebergRestContainer.getMappedPort(8181));
    assertThat(catalog.getSource()).isEqualTo("spark");
  }

  private void clearTables(String... tables) {
    Arrays.asList(tables).stream()
        .filter(t -> spark.catalog().tableExists(t))
        .forEach(t -> spark.sql("DROP TABLE IF EXISTS " + t));
  }

  @SuppressWarnings("PMD.UnusedPrivateMethod")
  private static boolean isJava8AndSpark35() {
    return System.getProperty("java.version").startsWith("1.8")
        && System.getProperty(SPARK_VERSION).startsWith("3.5");
  }
}
