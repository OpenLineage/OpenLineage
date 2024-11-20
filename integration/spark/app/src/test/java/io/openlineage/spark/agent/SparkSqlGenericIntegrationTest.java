/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent;

import static io.openlineage.spark.agent.MockServerUtils.getEventsEmitted;
import static io.openlineage.spark.agent.SparkTestUtils.SPARK_VERSION;
import static org.assertj.core.api.Assertions.assertThat;

import io.openlineage.client.OpenLineage.InputDataset;
import io.openlineage.client.OpenLineage.InputDatasetInputFacets;
import io.openlineage.client.OpenLineage.InputStatisticsInputDatasetFacet;
import io.openlineage.client.OpenLineage.OutputDataset;
import io.openlineage.client.OpenLineage.OutputDatasetOutputFacets;
import io.openlineage.client.OpenLineage.OutputStatisticsOutputDatasetFacet;
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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.mockserver.integration.ClientAndServer;

/**
 * This class contains Spark non-container integration tests that do not fit into other integration
 * test classes.
 */
@Tag("integration-test")
@Slf4j
class SparkSqlGenericIntegrationTest {

  @SuppressWarnings("PMD")
  private static final String LOCAL_IP = "127.0.0.1";

  private static final int MOCK_SERVER_PORT = 1083;
  private static SparkSession spark;
  private static ClientAndServer mockServer;

  @BeforeAll
  @SneakyThrows
  public static void beforeAll() {
    SparkSession$.MODULE$.cleanupAnyExistingSession();
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
            .appName("GenericSqlIntegrationTest")
            .config("spark.driver.host", LOCAL_IP)
            .config("spark.driver.bindAddress", LOCAL_IP)
            .config("spark.sql.shuffle.partitions", 1)
            .config("spark.openlineage.transport.type", "http")
            .config(
                "spark.openlineage.transport.url",
                "http://localhost:" + mockServer.getPort() + "/api/v1/lineage")
            .config("spark.openlineage.facets.debug.disabled", "false")
            .config("spark.openlineage.namespace", "generic-namespace")
            .config("spark.openlineage.parentJobNamespace", "parent-namespace")
            .config("spark.extraListeners", OpenLineageSparkListener.class.getName())
            .enableHiveSupport()
            .getOrCreate();
  }

  @Test
  @EnabledIfSystemProperty(named = SPARK_VERSION, matches = "([34].*)") // Spark version >= 3.*
  @SneakyThrows
  void sparkSqlQueryEmitsInputAndOutputStatistics() {
    String warehouseDir = spark.sparkContext().conf().get("spark.sql.warehouse.dir");
    FileUtils.deleteDirectory(new File(warehouseDir, "test_input1"));
    FileUtils.deleteDirectory(new File(warehouseDir, "test_input2"));

    // write 100 rows to test_input1
    createTempDataset(100).write().mode("overwrite").saveAsTable("test_input1");

    // write 50 rows to test_input2
    createTempDataset(50).write().mode("overwrite").saveAsTable("test_input2");

    // write a union of both inputs
    spark.sql("DROP TABLE IF EXISTS test_output");
    spark.sql(
        "CREATE TABLE test_output AS SELECT * FROM test_input1 UNION ALL SELECT * FROM test_input2");
    spark.stop();
    List<RunEvent> events = getEventsEmitted(mockServer);

    // verify output statistics facet
    Optional<OutputStatisticsOutputDatasetFacet> outputStatistics =
        events.stream()
            .filter(e -> !e.getOutputs().isEmpty())
            .map(e -> e.getOutputs().get(0))
            .filter(e -> e.getName().endsWith("test_output"))
            .map(OutputDataset::getOutputFacets)
            .map(OutputDatasetOutputFacets::getOutputStatistics)
            .filter(Objects::nonNull)
            .findFirst();

    assertThat(outputStatistics).isPresent();
    assertThat(outputStatistics.get().getRowCount()).isEqualTo(50 + 100);
    assertThat(outputStatistics.get().getSize()).isGreaterThan(0);
    assertThat(outputStatistics.get().getFileCount()).isGreaterThan(0);

    // verify input1 statistics facet
    Optional<InputStatisticsInputDatasetFacet> inputStatistics1 =
        events.stream()
            .filter(e -> !e.getInputs().isEmpty())
            .flatMap(e -> e.getInputs().stream())
            .filter(e -> e.getName().endsWith("test_input1"))
            .map(InputDataset::getInputFacets)
            .map(InputDatasetInputFacets::getInputStatistics)
            .filter(Objects::nonNull)
            .findFirst();

    assertThat(inputStatistics1).isPresent();
    // Row count is not working for non V2 relations
    assertThat(inputStatistics1.get().getSize()).isGreaterThan(0);
    assertThat(inputStatistics1.get().getFileCount()).isEqualTo(1);

    // verify input2 statistics facet
    Optional<InputStatisticsInputDatasetFacet> inputStatistics2 =
        events.stream()
            .filter(e -> !e.getInputs().isEmpty())
            .flatMap(e -> e.getInputs().stream())
            .filter(e -> e.getName().endsWith("test_input2"))
            .map(InputDataset::getInputFacets)
            .map(InputDatasetInputFacets::getInputStatistics)
            .filter(Objects::nonNull)
            .findFirst();

    assertThat(inputStatistics2).isPresent();
    // Row count is not working for non V2 relations
    assertThat(inputStatistics2.get().getSize()).isGreaterThan(500);
    assertThat(inputStatistics2.get().getFileCount()).isEqualTo(1);
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
