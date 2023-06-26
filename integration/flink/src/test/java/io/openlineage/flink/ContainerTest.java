/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink;

import static java.nio.file.Files.readAllBytes;
import static org.awaitility.Awaitility.await;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.JsonBody.json;

import com.google.common.io.Resources;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockserver.client.MockServerClient;
import org.mockserver.matchers.MatchType;
import org.mockserver.model.HttpRequest;
import org.mockserver.model.JsonBody;
import org.mockserver.model.RequestDefinition;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.MockServerContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@SuppressWarnings("PMD")
@Tag("integration-test")
@Testcontainers
@Slf4j
class ContainerTest {

  private static final Network network = Network.newNetwork();
  private static MockServerClient mockServerClient;

  @Container
  private static final MockServerContainer openLineageClientMockContainer =
      FlinkContainerUtils.makeMockServerContainer(network);

  @Container
  private static final GenericContainer zookeeper =
      FlinkContainerUtils.makeZookeeperContainer(network);

  @Container
  private static final GenericContainer kafka =
      FlinkContainerUtils.makeKafkaContainer(network, zookeeper);

  @Container
  private static final GenericContainer schemaRegistry =
      FlinkContainerUtils.makeSchemaRegistryContainer(network, kafka);

  @Container
  private static final GenericContainer generateEvents =
      FlinkContainerUtils.makeGenerateEventsContainer(network, schemaRegistry);

  private static GenericContainer jobManager;

  private static GenericContainer taskManager;

  @BeforeEach
  public void setup() {
    mockServerClient =
        new MockServerClient(
            openLineageClientMockContainer.getHost(),
            openLineageClientMockContainer.getServerPort());
    mockServerClient
        .when(request("/api/v1/lineage"))
        .respond(org.mockserver.model.HttpResponse.response().withStatusCode(201));

    await().until(openLineageClientMockContainer::isRunning);
  }

  @AfterEach
  public void cleanup() {
    mockServerClient.reset();
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
        Arrays.asList(
            openLineageClientMockContainer,
            zookeeper,
            schemaRegistry,
            kafka,
            generateEvents,
            jobManager,
            taskManager));

    network.close();
  }

  void runUntilCheckpoint(String jobName, Properties jobProperties) {
    jobManager =
        FlinkContainerUtils.makeFlinkJobManagerContainer(
            jobName,
            network,
            Arrays.asList(generateEvents, openLineageClientMockContainer),
            jobProperties);
    taskManager =
        FlinkContainerUtils.makeFlinkTaskManagerContainer(network, Arrays.asList(jobManager));
    taskManager.start();
    await()
        .atMost(Duration.ofMinutes(5))
        .until(() -> FlinkContainerUtils.verifyJobManagerReachedCheckpointOrFinished(jobManager));
  }

  void runUntilFailed(String jobName) {
    jobManager =
        FlinkContainerUtils.makeFlinkJobManagerContainer(
            jobName,
            network,
            Arrays.asList(generateEvents, openLineageClientMockContainer),
            new Properties());
    taskManager =
        FlinkContainerUtils.makeFlinkTaskManagerContainer(network, Arrays.asList(jobManager));
    taskManager.start();
    await().atMost(Duration.ofMinutes(5)).until(() -> !jobManager.isRunning());
  }

  HttpRequest getEvent(String path) {
    return request()
        .withPath("/api/v1/lineage")
        .withBody(readJson(Path.of(Resources.getResource(path).getPath())));
  }

  void verify(String... eventFiles) {
    await()
        .atMost(Duration.ofSeconds(10))
        .pollInterval(Duration.ofSeconds(2))
        .untilAsserted(
            () ->
                mockServerClient.verify(
                    Arrays.stream(eventFiles)
                        .map(this::getEvent)
                        .collect(Collectors.toList())
                        .toArray(new RequestDefinition[0])));
  }

  @Test
  @SneakyThrows
  void testOpenLineageEventSentForKafkaJob() {
    runUntilCheckpoint("io.openlineage.flink.FlinkStatefulApplication", new Properties());
    verify("events/expected_kafka.json", "events/expected_kafka_checkpoints.json");
  }

  @Test
  @SneakyThrows
  void testOpenLineageEventSentForKafkaSourceWithGenericRecord() {
    runUntilCheckpoint(
        "io.openlineage.flink.FlinkSourceWithGenericRecordApplication", new Properties());
    verify("events/expected_kafka_generic_record.json");
  }

  @Test
  @SneakyThrows
  void testOpenLineageEventSentForKafkaJobWithTopicPattern() {
    Properties jobProperties = new Properties();
    jobProperties.put("inputTopics", "io.openlineage.flink.kafka.input.*");
    runUntilCheckpoint("io.openlineage.flink.FlinkStatefulApplication", jobProperties);
    verify("events/expected_kafka.json", "events/expected_kafka_checkpoints.json");
  }

  @Test
  @SneakyThrows
  void testOpenLineageEventSentForLegacyKafkaJob() {
    runUntilCheckpoint("io.openlineage.flink.FlinkLegacyKafkaApplication", new Properties());
    verify("events/expected_legacy_kafka.json");
  }

  @Test
  @SneakyThrows
  void testOpenLineageEventSentForLegacyKafkaJobWithTopicPattern() {
    Properties jobProperties = new Properties();
    jobProperties.put("inputTopics", "io.openlineage.flink.kafka.input.*");
    runUntilCheckpoint("io.openlineage.flink.FlinkLegacyKafkaApplication", jobProperties);
    verify("events/expected_legacy_kafka.json");
  }

  @Test
  @SneakyThrows
  void testOpenLineageEventSentForIcebergJob() {
    runUntilCheckpoint("io.openlineage.flink.FlinkIcebergApplication", new Properties());
    verify("events/expected_iceberg.json");
  }

  @Test
  @SneakyThrows
  void testOpenLineageFailedEventSentForFailedJob() {
    runUntilFailed("io.openlineage.flink.FlinkFailedApplication");
    verify("events/expected_failed.json");
  }

  @SneakyThrows
  private JsonBody readJson(Path path) {
    return json(new String(readAllBytes(path)), MatchType.ONLY_MATCHING_FIELDS);
  }
}
