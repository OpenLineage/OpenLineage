/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent;

import static io.openlineage.spark.agent.MockServerUtils.getEventsEmitted;
import static org.assertj.core.api.Assertions.assertThat;

import io.openlineage.client.OpenLineage.RunEvent;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
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

@Tag("integration-test")
@Slf4j
class SparkRddGenericIntegrationTest {

  @SuppressWarnings("PMD")
  private static final String LOCAL_IP = "127.0.0.1";

  private static final int MOCK_SERVER_PORT = 1083;
  private static SparkSession spark;
  private static ClientAndServer mockServer;

  @BeforeAll
  @SneakyThrows
  public static void beforeAll() {
    Spark4CompatUtils.cleanupAnyExistingSession();
    mockServer = MockServerUtils.createAndConfigureMockServer(MOCK_SERVER_PORT);
  }

  @AfterAll
  @SneakyThrows
  public static void afterAll() {
    Spark4CompatUtils.cleanupAnyExistingSession();
    MockServerUtils.stopMockServer(mockServer);
  }

  @BeforeEach
  @SneakyThrows
  public void beforeEach() {
    MockServerUtils.clearRequests(mockServer);
  }

  @Test
  void sparkEmitsRDDEventsByDefault() {
    setupSparkSession();
    Dataset<Row> df = createTempDataset(3);
    df.write().mode("overwrite").option("header", "true").csv("/tmp/test_data/temp_input/");

    Dataset<Row> dfRead =
        spark
            .read()
            .option("inferSchema", "true")
            .option("header", "true")
            .csv("/tmp/test_data/temp_input/");

    Dataset<Row> agg = dfRead.groupBy("a").count();
    agg.write().mode("overwrite").csv("/tmp/test_data/final_output/");

    spark.stop();

    List<RunEvent> events = getEventsEmitted(mockServer);

    assertThat(
            events.stream()
                .map(event -> event.getJob().getFacets().getJobType().getJobType())
                .filter("RDD_JOB"::equals)
                .collect(Collectors.toSet()))
        .hasSize(1);

    assertThat(
            events.stream()
                .filter(event -> RunEvent.EventType.COMPLETE.equals(event.getEventType()))
                .filter(event -> event.getJob().getName().endsWith(".test_data_final_output"))
                .collect(Collectors.toSet()))
        .hasSize(1);
  }

  @Test
  void sparkDoesNotEmitRDDEventsWhenDisabled() {
    setupSparkSessionWithRddDisabled();
    Dataset<Row> df = createTempDataset(3);
    df.write().mode("overwrite").option("header", "true").csv("/tmp/test_data/temp_input/");

    Dataset<Row> dfRead =
        spark
            .read()
            .option("inferSchema", "true")
            .option("header", "true")
            .csv("/tmp/test_data/temp_input/");

    Dataset<Row> agg = dfRead.groupBy("a").count();
    agg.write().mode("overwrite").csv("/tmp/test_data/final_output/");

    spark.stop();

    List<RunEvent> events = getEventsEmitted(mockServer);

    assertThat(
            events.stream()
                .map(event -> event.getJob().getFacets().getJobType().getJobType())
                .filter("RDD_JOB"::equals)
                .collect(Collectors.toSet()))
        .hasSize(0);

    assertThat(
            events.stream()
                .filter(event -> RunEvent.EventType.COMPLETE.equals(event.getEventType()))
                .filter(event -> event.getJob().getName().endsWith(".test_data_final_output"))
                .collect(Collectors.toSet()))
        .hasSize(1);
  }

  private static void setupSparkSession() {
    spark = getSparkSessionBuilder().getOrCreate();
  }

  private static void setupSparkSessionWithRddDisabled() {
    spark =
        getSparkSessionBuilder()
            .config("spark.openlineage.filter.rddEventsDisabled", true)
            .getOrCreate();
  }

  private static SparkSession.Builder getSparkSessionBuilder() {
    return SparkSession.builder()
        .master("local[*]")
        .appName("RddGenericIntegrationTest")
        .config("spark.driver.host", LOCAL_IP)
        .config("spark.driver.bindAddress", LOCAL_IP)
        .config("spark.sql.shuffle.partitions", 1)
        .config("spark.openlineage.transport.type", "http")
        .config(
            "spark.openlineage.transport.url",
            "http://localhost:" + mockServer.getPort() + "/api/v1/lineage")
        .config("spark.openlineage.facets.debug.disabled", "false")
        .config("spark.openlineage.namespace", "rdd-generic-namespace")
        .config("spark.openlineage.parentJobName", "parent-job")
        .config("spark.openlineage.parentRunId", "bd9c2467-3ed7-4fdc-85c2-41ebf5c73b40")
        .config("spark.openlineage.parentJobNamespace", "parent-namespace")
        .config("spark.openlineage.job.tags", "spark;key:value")
        .config("spark.openlineage.job.owners.team", "MyTeam")
        .config("spark.openlineage.job.owners.person", "John Smith")
        .config("spark.openlineage.run.tags", "run;set:up:SERVICE")
        .config("spark.openlineage.parentJobNamespace", "parent-namespace")
        .config("spark.extraListeners", OpenLineageSparkListener.class.getName());
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
