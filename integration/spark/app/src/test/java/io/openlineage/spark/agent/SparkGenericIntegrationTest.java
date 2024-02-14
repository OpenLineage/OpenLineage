/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent;

import static io.openlineage.spark.agent.MockServerUtils.getEmittedEvents;
import static io.openlineage.spark.agent.MockServerUtils.verifyEvents;
import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableList;
import io.openlineage.client.OpenLineage;
import java.nio.file.Files;
import java.nio.file.Path;
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

/**
 * This class contains Spark non-container integration tests that do not fit into other integration
 * test classes.
 */
@Tag("integration-test")
@Slf4j
class SparkGenericIntegrationTest {
  @SuppressWarnings("PMD")
  private static final String LOCAL_IP = "127.0.0.1";

  private static SparkSession spark;

  private static final Path TEST_RESULTS_DIR = Paths.get(System.getProperty("test.results.dir"));
  private static final Path TEST_CLASS_RESULTS_DIR =
      TEST_RESULTS_DIR.resolve(SparkGenericIntegrationTest.class.getSimpleName());
  private static final Path TEST_RESULTS_FILE_PATH =
      TEST_CLASS_RESULTS_DIR.resolve("sparkEmitsApplicationLevelEvents.json");

  @BeforeAll
  @SneakyThrows
  public static void beforeAll() {
    SparkSession$.MODULE$.cleanupAnyExistingSession();
  }

  @AfterAll
  @SneakyThrows
  public static void afterAll() {
    SparkSession$.MODULE$.cleanupAnyExistingSession();
  }

  @BeforeEach
  @SneakyThrows
  public void beforeEach() {
    Files.deleteIfExists(TEST_RESULTS_FILE_PATH);
    Files.createDirectories(TEST_CLASS_RESULTS_DIR);
    spark =
        SparkSession.builder()
            .master("local[*]")
            .appName("GenericIntegrationTest")
            .config("spark.driver.host", LOCAL_IP)
            .config("spark.driver.bindAddress", LOCAL_IP)
            .config("spark.sql.shuffle.partitions", 1)
            .config("spark.openlineage.transport.type", "file")
            .config("spark.openlineage.transport.location", TEST_RESULTS_FILE_PATH.toString())
            .config("spark.openlineage.facets.disabled", "spark_unknown;spark.logicalPlan")
            .config("spark.openlineage.debugFacet", "disabled")
            .config("spark.openlineage.namespace", "generic-namespace")
            .config("spark.openlineage.parentJobName", "parent-job")
            .config("spark.openlineage.parentRunId", "bd9c2467-3ed7-4fdc-85c2-41ebf5c73b40")
            .config("spark.openlineage.parentJobNamespace", "parent-namespace")
            .config("spark.extraListeners", OpenLineageSparkListener.class.getName())
            .getOrCreate();
  }

  @SneakyThrows
  @Test
  void sparkEmitsApplicationLevelEvents() {
    Dataset<Row> df = createTempDataset();

    Dataset<Row> agg = df.groupBy("a").count();
    agg.write().mode("overwrite").csv("/tmp/test_data/test_output/");

    spark.stop();
    verifyEvents(
        TEST_RESULTS_FILE_PATH,
        node ->
            !node.get("eventType")
                .asText()
                .equals("RUNNING"), // ignore RUNNING events from the comparison
        "applicationLevelStartApplication.json",
        "applicationLevelStartJob.json",
        "applicationLevelCompleteJob.json",
        "applicationLevelCompleteApplication.json");

    List<OpenLineage.RunEvent> events = getEmittedEvents(TEST_RESULTS_FILE_PATH);
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
