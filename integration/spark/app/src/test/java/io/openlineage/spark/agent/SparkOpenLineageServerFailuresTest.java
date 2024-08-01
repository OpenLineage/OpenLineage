/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent;

import static org.mockserver.model.HttpRequest.request;

import java.util.Collections;
import lombok.extern.slf4j.Slf4j;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockserver.client.MockServerClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.MockServerContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Class containing tests to verify that Spark job executes properly even when problems related to
 * OpenLineage server encountered
 */
@Slf4j
@Tag("integration-test")
@Testcontainers
class SparkOpenLineageServerFailuresTest {

  private static final Network network = Network.newNetwork();

  @Container
  private static final MockServerContainer openLineageClientMockContainer =
      SparkContainerUtils.makeMockServerContainer(network);

  private static final String INCORRECT_PORT = "3000";
  private static final String CORRECT_PORT = "1080";
  private static final Logger logger =
      LoggerFactory.getLogger(SparkOpenLineageServerFailuresTest.class);

  private static MockServerClient mockServerClient;

  @BeforeAll
  public static void setup() {
    mockServerClient =
        new MockServerClient(
            openLineageClientMockContainer.getHost(),
            openLineageClientMockContainer.getServerPort());

    Awaitility.await().until(openLineageClientMockContainer::isRunning);
  }

  @AfterEach
  public void cleanup() {
    mockServerClient.reset();
  }

  @AfterAll
  public static void tearDown() {
    try {
      openLineageClientMockContainer.stop();
    } catch (Exception e) {
      logger.error("Unable to shut down openlineage client container", e);
    }
    network.close();
  }

  @Test
  @SuppressWarnings("PMD") // waiting for WaitMessage serves as an assertion
  void testIncorrectOpenLineageEndpointUrl() {
    SparkContainerUtils.makePysparkContainerWithDefaultConf(
            network,
            "http://openlineageclient:" + INCORRECT_PORT,
            ".*Spark is fine!.*",
            openLineageClientMockContainer,
            "testFailures",
            Collections.emptyList(),
            Collections.emptyList(),
            "/opt/spark_scripts/spark_test_failures.py")
        .start();
  }

  @Test
  @SuppressWarnings("PMD") // waiting for WaitMessage serves as an assertion
  void testOpenLineageEndpointResponds400() {
    configureMockServerToRespondWith(400);

    SparkContainerUtils.makePysparkContainerWithDefaultConf(
            network,
            "http://openlineageclient:" + CORRECT_PORT,
            ".*Spark is fine!.*",
            openLineageClientMockContainer,
            "testFailures",
            Collections.emptyList(),
            Collections.emptyList(),
            "/opt/spark_scripts/spark_test_failures.py")
        .start();
  }

  @Test
  @SuppressWarnings("PMD") // waiting for WaitMessage serves as an assertion
  void testOpenLineageEndpointResponds500() {
    configureMockServerToRespondWith(500);

    SparkContainerUtils.makePysparkContainerWithDefaultConf(
            network,
            "http://openlineageclient:" + CORRECT_PORT,
            ".*Spark is fine!.*",
            openLineageClientMockContainer,
            "testFailures",
            Collections.emptyList(),
            Collections.emptyList(),
            "/opt/spark_scripts/spark_test_failures.py")
        .start();
  }

  private void configureMockServerToRespondWith(int statusCode) {
    mockServerClient
        .when(request("/api/v1/lineage"))
        .respond(org.mockserver.model.HttpResponse.response().withStatusCode(statusCode));
  }
}
