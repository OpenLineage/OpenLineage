/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent;

import static io.openlineage.spark.agent.MockServerUtils.getEventsEmitted;
import static io.openlineage.spark.agent.MockServerUtils.verifyEvents;
import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableList;
import io.openlineage.client.OpenLineage;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;
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
import org.mockserver.integration.ClientAndServer;

/**
 * This class contains Spark non-container integration tests that do not fit into other integration
 * test classes.
 */
@Tag("integration-test")
@Slf4j
public class SparkGenericIntegrationTest {

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

    java.nio.file.Path resourcesDir = Paths.get(System.getProperty("resources.dir"));

    java.nio.file.Path log4j = resourcesDir.resolve("log4j.properties").toAbsolutePath();
    java.nio.file.Path log4j2 = resourcesDir.resolve("log4j2.properties").toAbsolutePath();

    System.setProperty("log4j.configuration", log4j.toString());
    System.setProperty("log4j.configurationFile", log4j2.toString());
    System.setProperty("log4j2.configurationFile", log4j2.toString());

    spark =
        SparkSession.builder()
            .master("local[*]")
            .appName("GenericIntegrationTest")
            .config("spark.driver.host", LOCAL_IP)
            .config("spark.driver.bindAddress", LOCAL_IP)
            .config("spark.sql.shuffle.partitions", 1)
            .config("spark.openlineage.transport.type", "http")
            .config(
                "spark.openlineage.transport.url",
                "http://localhost:" + mockServer.getPort() + "/api/v1/lineage")
            .config("spark.openlineage.facets.disabled", "spark_unknown;spark.logicalPlan")
            .config("spark.openlineage.debugFacet", "disabled")
            .config("spark.openlineage.namespace", "generic-namespace")
            .config("spark.openlineage.parentJobName", "parent-job")
            .config("spark.openlineage.parentRunId", "bd9c2467-3ed7-4fdc-85c2-41ebf5c73b40")
            .config("spark.openlineage.parentJobNamespace", "parent-namespace")
            .config("spark.extraListeners", OpenLineageSparkListener.class.getName())
            .getOrCreate();
  }

  @Test
  void sparkEmitsApplicationLevelEvents() {
    Dataset<Row> df = createTempDataset();

    Dataset<Row> agg = df.groupBy("a").count();
    agg.write().mode("overwrite").csv("/tmp/test_data/test_output/");

    spark.stop();
    verifyEvents(
        mockServer,
        "applicationLevelStartApplication.json",
        "applicationLevelStartJob.json",
        "applicationLevelCompleteJob.json",
        "applicationLevelCompleteApplication.json");

    List<OpenLineage.RunEvent> events = getEventsEmitted(mockServer);
    assertThat(
            events.stream()
                .map(
                    event -> {
                      if (event.getJob().getName().equals("generic_integration_test")) {
                        return event.getRun().getRunId();
                      } else {
                        return event.getRun().getFacets().getParent().getRun().getRunId();
                      }
                    })
                .collect(Collectors.toSet()))
        .hasSize(1);
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
}
