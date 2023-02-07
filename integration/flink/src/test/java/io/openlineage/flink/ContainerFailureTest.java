/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink;

import static org.awaitility.Awaitility.await;
import static org.mockserver.model.HttpRequest.request;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockserver.client.MockServerClient;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.MockServerContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@SuppressWarnings("PMD")
@Tag("integration-test")
@Testcontainers
@Slf4j
class ContainerFailureTest {

  private static final String FAKE_APPLICATION = "io.openlineage.flink.FlinkFakeApplication";
  private static final String NEW_CHECKPOINT_ENCOUNTERED = "New checkpoint encountered";
  private static final String NETWORK_PARTITION_CONFIG =
      "/opt/flink/lib/openlineage-network-partition.yml";

  protected static final Network network = Network.newNetwork();
  protected static MockServerClient mockServerClient;

  @Container
  protected static final MockServerContainer openLineageClientMockContainer =
      FlinkContainerUtils.makeMockServerContainer(network);

  protected static GenericContainer jobManager;

  protected static GenericContainer taskManager;

  void runUntilCheckpoint(String jobName, String configPath) {
    Properties jobProperties = new Properties();
    jobProperties.put("configPath", configPath);
    jobManager =
        FlinkContainerUtils.makeFlinkJobManagerContainer(
            jobName, network, Collections.emptyList(), jobProperties);
    taskManager =
        FlinkContainerUtils.makeFlinkTaskManagerContainer(
            network, Collections.singletonList(jobManager));
    taskManager.start();
    await()
        .atMost(Duration.ofMinutes(5))
        .until(() -> FlinkContainerUtils.verifyJobManagerReachedCheckpointOrFinished(jobManager));
  }

  @Test
  @SneakyThrows
  void testEmitFailedNetworkPartitionDoesNotKillFlinkJob() {
    Properties jobProperties = new Properties();
    jobProperties.put("configPath", NETWORK_PARTITION_CONFIG);
    jobManager =
        FlinkContainerUtils.makeFlinkJobManagerContainer(
            FAKE_APPLICATION, network, Collections.emptyList(), jobProperties);
    taskManager =
        FlinkContainerUtils.makeFlinkTaskManagerContainer(
            network, Collections.singletonList(jobManager));
    taskManager.start();
    await()
        .atMost(Duration.ofMinutes(5))
        .until(() -> jobManager.getLogs().contains(NEW_CHECKPOINT_ENCOUNTERED));

    runUntilCheckpoint(FAKE_APPLICATION, NETWORK_PARTITION_CONFIG);
  }

  // TODO: write testCrashingLineageProviderDoesNotKillFlinkJob

  @Test
  @SneakyThrows
  void testEmitFailed500DoesNotKillFlinkJob() {
    mockServerClient =
        new MockServerClient(
            openLineageClientMockContainer.getHost(),
            openLineageClientMockContainer.getServerPort());
    mockServerClient
        .when(request("/api/v1/lineage"))
        .respond(org.mockserver.model.HttpResponse.response().withStatusCode(500));

    await().until(openLineageClientMockContainer::isRunning);

    jobManager =
        FlinkContainerUtils.makeFlinkJobManagerContainer(
            FAKE_APPLICATION, network, Collections.emptyList(), new Properties());
    taskManager =
        FlinkContainerUtils.makeFlinkTaskManagerContainer(
            network, Collections.singletonList(jobManager));
    taskManager.start();
    await()
        .atMost(Duration.ofMinutes(5))
        .until(() -> jobManager.getLogs().contains(NEW_CHECKPOINT_ENCOUNTERED));

    runUntilCheckpoint(FAKE_APPLICATION, NETWORK_PARTITION_CONFIG);
  }

  @AfterEach
  public void cleanup() {
    if (mockServerClient != null) {
      mockServerClient.reset();
    }
    try {
      if (taskManager != null) taskManager.stop();
    } catch (Exception e2) {
      log.error("Unable to shut down taskmanager container", e2);
    }
    try {
      if (jobManager != null) jobManager.stop();
    } catch (Exception e2) {
      log.error("Unable to shut down jobmanager container", e2);
    }
  }

  @AfterAll
  public static void tearDown() {
    FlinkContainerUtils.stopAll(
        Arrays.asList(openLineageClientMockContainer, jobManager, taskManager));

    network.close();
  }
}
