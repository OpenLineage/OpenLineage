/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockserver.model.HttpRequest.request;

import com.google.common.collect.ImmutableList;
import java.io.IOException;
import lombok.SneakyThrows;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
import org.mockserver.configuration.Configuration;
import org.mockserver.integration.ClientAndServer;
import org.slf4j.event.Level;

@Tag("integration-test")
class CircuitBreakerIntegrationTest {
  private static final int MOCKSERVER_PORT = 1086;
  private static final String LOCAL_IP = "127.0.0.1";
  static SparkSession spark;

  private static ClientAndServer mockServer;

  @BeforeAll
  @SneakyThrows
  public static void beforeAll() {
    SparkSession$.MODULE$.cleanupAnyExistingSession();
    Configuration configuration = new Configuration();
    configuration.logLevel(Level.ERROR);

    mockServer = ClientAndServer.startClientAndServer(configuration, MOCKSERVER_PORT);
    mockServer
        .when(request("/api/v1/lineage"))
        .respond(org.mockserver.model.HttpResponse.response().withStatusCode(201));
  }

  @AfterAll
  @SneakyThrows
  public static void afterAll() {
    SparkSession$.MODULE$.cleanupAnyExistingSession();
  }

  @BeforeEach
  @SneakyThrows
  public void beforeEach() {
    spark =
        SparkSession.builder()
            .master("local[*]")
            .appName("CircuitBreakerIntegrationTest")
            .config("spark.driver.host", LOCAL_IP)
            .config("spark.driver.bindAddress", LOCAL_IP)
            .config("spark.sql.shuffle.partitions", 1)
            .config("spark.openlineage.transport.type", "http")
            .config(
                "spark.openlineage.transport.url",
                "http://localhost:" + mockServer.getPort() + "/api/v1/namespaces/circuit-breaker")
            .config(
                "spark.openlineage.circuitBreaker.type", "static") // turn on test circuit breaker
            .config("spark.openlineage.circuitBreaker.valuesReturned", "true,true")
            .config("spark.extraListeners", OpenLineageSparkListener.class.getName())
            .getOrCreate();

    spark.sparkContext().setLogLevel("WARN");
  }

  @Test
  void testCircuitBreakerIsChecked() throws IOException {
    FileSystem.get(spark.sparkContext().hadoopConfiguration())
        .delete(new Path("/tmp/circuit-breaker"), true);

    Dataset<Row> dataset =
        spark
            .createDataFrame(
                ImmutableList.of(RowFactory.create(1L, 2L), RowFactory.create(3L, 4L)),
                new StructType(
                    new StructField[] {
                      new StructField("a", LongType$.MODULE$, false, Metadata.empty()),
                      new StructField("b", LongType$.MODULE$, false, Metadata.empty())
                    }))
            .repartition(1);

    dataset.write().save("/tmp/circuit-breaker");

    assertThat(mockServer.retrieveRecordedRequests(request().withPath("/api/v1/lineage")))
        .isEmpty();
  }
}
