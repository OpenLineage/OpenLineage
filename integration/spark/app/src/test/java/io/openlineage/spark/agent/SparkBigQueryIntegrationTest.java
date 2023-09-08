/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent;

import static io.openlineage.spark.agent.MockServerUtils.verifyEvents;
import static org.mockserver.model.HttpRequest.request;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.mockserver.client.MockServerClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.MockServerContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Tag("integration-test")
@Tag("bigquery")
@Testcontainers
class SparkBigQueryIntegrationTest {

  private static final Network network = Network.newNetwork();

  @Container
  private static final MockServerContainer openLineageClientMockContainer =
      SparkContainerUtils.makeMockServerContainer(network);

  private static final String SPARK_3 = "(3.*)";
  private static final String SPARK_VERSION = "spark.version";
  private static MockServerClient mockServerClient;
  private static final Logger logger = LoggerFactory.getLogger(SparkBigQueryIntegrationTest.class);

  @BeforeAll
  public static void setup() {
    mockServerClient =
        new MockServerClient(
            openLineageClientMockContainer.getHost(),
            openLineageClientMockContainer.getServerPort());
    mockServerClient
        .when(request("/api/v1/lineage"))
        .respond(org.mockserver.model.HttpResponse.response().withStatusCode(201));

    Awaitility.await().until(openLineageClientMockContainer::isRunning);
  }

  @AfterEach
  public void cleanupSpark() {
    mockServerClient.reset();
  }

  @AfterAll
  public static void tearDown() {
    try {
      openLineageClientMockContainer.stop();
    } catch (Exception e2) {
      logger.error("Unable to shut down openlineage client container", e2);
    }
    network.close();
  }

  @Test
  @EnabledIfEnvironmentVariable(named = "CI", matches = "true")
  @EnabledIfSystemProperty(named = SPARK_VERSION, matches = SPARK_3) // Spark version >= 3.*
  void testReadAndWriteFromBigquery() {
    List<String> sparkConfigParams = new ArrayList<>();
    sparkConfigParams.add(
        "spark.hadoop.google.cloud.auth.service.account.json.keyfile=/opt/gcloud/gcloud-service-key.json");
    sparkConfigParams.add("spark.hadoop.google.cloud.auth.service.account.enable=true");
    sparkConfigParams.add(
        "spark.hadoop.fs.AbstractFileSystem.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS");
    sparkConfigParams.add(
        "spark.hadoop.fs.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem");

    SparkContainerUtils.runPysparkContainerWithDefaultConf(
        network,
        openLineageClientMockContainer,
        "testReadAndWriteFromBigquery",
        Collections.emptyList(),
        sparkConfigParams,
        "spark_bigquery.py");
    verifyEvents(
        mockServerClient,
        Collections.singletonMap(
            "{spark_version}", System.getProperty(SPARK_VERSION).replace(".", "_")),
        "pysparkBigquerySaveStart.json",
        "pysparkBigqueryInsertStart.json",
        "pysparkBigqueryInsertEnd.json",
        "pysparkBigquerySaveEnd.json");
  }
}
