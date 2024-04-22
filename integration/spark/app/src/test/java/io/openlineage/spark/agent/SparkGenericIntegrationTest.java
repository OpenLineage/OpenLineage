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
import io.openlineage.client.OpenLineage.OwnershipJobFacetOwners;
import io.openlineage.client.OpenLineage.RunEvent;
import io.openlineage.spark.agent.lifecycle.UnknownEntryFacetListener;
import java.util.List;
import java.util.Map;
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
class SparkGenericIntegrationTest {

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
            .appName("GenericIntegrationTest")
            .config("spark.driver.host", LOCAL_IP)
            .config("spark.driver.bindAddress", LOCAL_IP)
            .config("spark.sql.shuffle.partitions", 1)
            .config("spark.openlineage.transport.type", "http")
            .config(
                "spark.openlineage.transport.url",
                "http://localhost:" + mockServer.getPort() + "/api/v1/lineage")
            .config("spark.openlineage.facets.disabled", "spark_unknown;spark.logicalPlan")
            .config("spark.openlineage.debugFacet", "enabled")
            .config("spark.openlineage.namespace", "generic-namespace")
            .config("spark.openlineage.parentJobName", "parent-job")
            .config("spark.openlineage.parentRunId", "bd9c2467-3ed7-4fdc-85c2-41ebf5c73b40")
            .config("spark.openlineage.parentJobNamespace", "parent-namespace")
            .config("spark.openlineage.job.owners.team", "MyTeam")
            .config("spark.openlineage.job.owners.person", "John Smith")
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
                      if ("generic_integration_test".equals(event.getJob().getName())) {
                        return event.getRun().getRunId();
                      } else {
                        return event.getRun().getFacets().getParent().getRun().getRunId();
                      }
                    })
                .collect(Collectors.toSet()))
        .hasSize(1);

    // test UnknownEntryFacetListener clears its static list of visited nodes
    assertThat(UnknownEntryFacetListener.getInstance().getVisitedNodesSize()).isEqualTo(0);
  }

  @Test
  @SneakyThrows
  @SuppressWarnings("PMD.JUnitTestsShouldIncludeAssert")
  void sparkGathersMetrics() {
    Dataset<Row> df = createTempDataset();

    Dataset<Row> agg = df.groupBy("a").count();
    agg.write().mode("overwrite").csv("/tmp/test_data/test_output/");
    spark.stop();
    List<OpenLineage.RunEvent> events = getEventsEmitted(mockServer);
    events.stream()
        .map(
            event -> {
              Object debugFacet = event.getRun().getFacets().getAdditionalProperties().get("debug");
              if (debugFacet != null) {
                Object metricsFacet =
                    ((OpenLineage.DefaultRunFacet) debugFacet)
                        .getAdditionalProperties()
                        .get("metrics");
                List<Map<String, Object>> metrics =
                    (List<Map<String, Object>>) ((Map<String, Object>) metricsFacet).get("metrics");
                assertThat(
                        metrics.stream()
                            .filter(metric -> "openlineage.emit.start".equals(metric.get("name")))
                            .findFirst())
                    .isPresent();
                assertThat(
                        metrics.stream()
                            .filter(
                                metric -> "openlineage.emit.complete".equals(metric.get("name")))
                            .findFirst())
                    .isPresent();
              }
              return null;
            });
  }

  @Test
  void sparkEmitsJobOwnershipFacet() {
    Dataset<Row> df = createTempDataset();
    Dataset<Row> agg = df.groupBy("a").count();
    agg.write().mode("overwrite").csv("/tmp/test_data/test_output/");

    spark.stop();
    verifyEvents(mockServer, "applicationLevelStartJob.json");

    RunEvent event =
        getEventsEmitted(mockServer).stream()
            .filter(e -> !e.getJob().getName().equals("generic_integration_test"))
            .findFirst()
            .get();
    List<OwnershipJobFacetOwners> owners = event.getJob().getFacets().getOwnership().getOwners();
    assertThat(owners).hasSize(2);
    assertThat(
        owners.stream()
            .filter(o -> o.getType().equals("team") && o.getName().equals("MyTeam"))
            .findAny()
            .isPresent());
    assertThat(
        owners.stream()
            .filter(o -> o.getType().equals("person") && o.getName().equals("John Smith"))
            .findAny()
            .isPresent());
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
