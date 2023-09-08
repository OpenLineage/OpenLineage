/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent;

import static io.openlineage.spark.agent.MockServerUtils.verifyEvents;
import static org.mockserver.model.HttpRequest.request;

import lombok.SneakyThrows;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.mockserver.client.MockServerClient;
import org.mockserver.model.RegexBody;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.MockServerContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Tag("integration-test")
@Testcontainers
class SparkContainerIntegrationTest {

  private static final Network network = Network.newNetwork();

  @Container
  private static final MockServerContainer openLineageClientMockContainer =
      SparkContainerUtils.makeMockServerContainer(network);

  private static final String SPARK_3 = "(3.*)";
  private static final String SPARK_VERSION = "spark.version";

  private static GenericContainer<?> pyspark;
  private static MockServerClient mockServerClient;
  private static final Logger logger = LoggerFactory.getLogger(SparkContainerIntegrationTest.class);

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
    try {
      if (pyspark != null) pyspark.stop();
    } catch (Exception e2) {
      logger.error("Unable to shut down pyspark container", e2);
    }
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
  @EnabledIfSystemProperty(named = SPARK_VERSION, matches = SPARK_3) // Spark version >= 3.*
  void testSymlinksFacetForHiveCatalog() {
    SparkContainerUtils.runPysparkContainerWithDefaultConf(
        network, openLineageClientMockContainer, "symlinks", "spark_hive_catalog.py");
    verifyEvents(mockServerClient, "pysparkSymlinksComplete.json");
  }

  @Test
  @SneakyThrows
  void testFacetsDisable() {
    SparkContainerUtils.runPysparkContainerWithDefaultConf(
        network, openLineageClientMockContainer, "testFacetsDisable", "spark_facets_disable.py");

    // response should not contain any of the below
    String regex = "^((?!(spark_unknown|spark.logicalPlan|dataSource)).)*$";

    mockServerClient.verify(request().withPath("/api/v1/lineage").withBody(new RegexBody(regex)));
  }
}
