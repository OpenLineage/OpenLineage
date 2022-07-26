/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink;

import static org.awaitility.Awaitility.await;
import static org.mockserver.model.HttpRequest.request;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
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

@Tag("integration-test")
@Testcontainers
@Slf4j
public class ContainerFailureTest {

  private static final String FAKE_APPLICATION = "io.openlineage.flink.FlinkFakeApplication";
  private static final String MOCKSERVER_PARTITION_CONFIG = "/opt/flink/lib/openlineage.yml";
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
    jobManager =
        FlinkContainerUtils.makeFlinkJobManagerContainer(
            jobName, configPath, network, Collections.emptyList());
    taskManager =
        FlinkContainerUtils.makeFlinkTaskManagerContainer(
            network, Collections.singletonList(jobManager));
    taskManager.start();
    await()
        .atMost(Duration.ofMinutes(5))
        .until(() -> jobManager.getLogs().contains("New checkpoint encountered"));
  }

  @Test
  @SneakyThrows
  public void testEmitFailedNetworkPartitionDoesNotKillFlinkJob() {
    jobManager =
        FlinkContainerUtils.makeFlinkJobManagerContainer(
            FAKE_APPLICATION, NETWORK_PARTITION_CONFIG, network, Collections.emptyList());
    taskManager =
        FlinkContainerUtils.makeFlinkTaskManagerContainer(
            network, Collections.singletonList(jobManager));
    taskManager.start();
    await()
        .atMost(Duration.ofMinutes(5))
        .until(() -> jobManager.getLogs().contains("New checkpoint encountered"));

    runUntilCheckpoint("io.openlineage.flink.FlinkFakeApplication", NETWORK_PARTITION_CONFIG);
  }

  @Test
  @SneakyThrows
  public void testCrashingLineageProviderDoesNotKillFlinkJob() {
    jobManager =
        FlinkContainerUtils.makeFlinkJobManagerContainer(
            FAKE_APPLICATION, NETWORK_PARTITION_CONFIG, network, Collections.emptyList());
    taskManager =
        FlinkContainerUtils.makeFlinkTaskManagerContainer(
            network, Collections.singletonList(jobManager));
    taskManager.start();
    await()
        .atMost(Duration.ofMinutes(5))
        .until(() -> jobManager.getLogs().contains("New checkpoint encountered"));

    runUntilCheckpoint("io.openlineage.flink.FlinkFakeApplication", NETWORK_PARTITION_CONFIG);
  }

  @Test
  @SneakyThrows
  public void testEmitFailed500DoesNotKillFlinkJob() {
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
            FAKE_APPLICATION, NETWORK_PARTITION_CONFIG, network, Collections.emptyList());
    taskManager =
        FlinkContainerUtils.makeFlinkTaskManagerContainer(
            network, Collections.singletonList(jobManager));
    taskManager.start();
    await()
        .atMost(Duration.ofMinutes(5))
        .until(() -> jobManager.getLogs().contains("New checkpoint encountered"));

    runUntilCheckpoint("io.openlineage.flink.FlinkFakeApplication", NETWORK_PARTITION_CONFIG);
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
