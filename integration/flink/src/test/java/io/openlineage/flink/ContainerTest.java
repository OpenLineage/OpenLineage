/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink;

import static io.openlineage.flink.FlinkContainerUtils.FLINK_IMAGE;
import static io.openlineage.flink.FlinkContainerUtils.genericContainer;
import static io.openlineage.flink.FlinkContainerUtils.getExampleAppJarPath;
import static io.openlineage.flink.FlinkContainerUtils.getOpenLineageJarPath;
import static java.nio.file.Files.readAllBytes;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.JsonBody.json;

import com.google.common.io.Resources;
import java.nio.file.Path;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Properties;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
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

  void runUntilCheckpoint(String entrypointClass, Properties jobProperties) {
    jobManager =
        FlinkContainerUtils.makeFlinkJobManagerContainer(
            entrypointClass,
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

  void runUntilNotRunning(String entrypointClass) {
    jobManager =
        FlinkContainerUtils.makeFlinkJobManagerContainer(
            entrypointClass,
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
    jobProperties.put("jobName", "flink_topic_pattern");
    runUntilCheckpoint("io.openlineage.flink.FlinkStatefulApplication", jobProperties);
    verify(
        "events/expected_kafka_topic_pattern.json",
        "events/expected_kafka_topic_pattern_checkpoints.json");
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
    jobProperties.put("jobName", "flink_legacy_kafka_topic_pattern");
    runUntilCheckpoint("io.openlineage.flink.FlinkLegacyKafkaApplication", jobProperties);
    verify("events/expected_legacy_kafka_topic_pattern.json");
  }

  @Test
  @SneakyThrows
  void testOpenLineageEventSentForIcebergJob() {
    runUntilCheckpoint("io.openlineage.flink.FlinkIcebergApplication", new Properties());
    verify("events/expected_iceberg.json");

    // verify input dataset is available only once
    HttpRequest request =
        mockServerClient.retrieveRecordedRequests(this.getEvent("events/expected_iceberg.json"))[0];

    assertThat(StringUtils.countMatches(request.getBodyAsString(), "tmp/warehouse/db/source"))
        .isEqualTo(1);
  }

  @Test
  @SneakyThrows
  void testOpenLineageFailedEventSentForFailedJob() {
    runUntilNotRunning("io.openlineage.flink.FlinkFailedApplication");
    verify("events/expected_failed.json");
  }

  @Test
  @SneakyThrows
  void testFlinkConfigApplication() {
    String inputTopics = "io.openlineage.flink.kafka.input1,io.openlineage.flink.kafka.input2";
    GenericContainer<?> jobManager =
        genericContainer(network, FLINK_IMAGE, "jobmanager")
            .withExposedPorts(8081)
            .withFileSystemBind(getOpenLineageJarPath(), "/opt/flink/lib/openlineage.jar")
            .withFileSystemBind(getExampleAppJarPath(), "/opt/flink/lib/example-app.jar")
            .withCommand(
                "standalone-job "
                    + String.format(
                        "--job-classname %s ", "io.openlineage.flink.FlinkStatefulApplication")
                    + "--input-topics "
                    + inputTopics
                    + " --output-topic io.openlineage.flink.kafka.output --job-name flink_conf_job")
            .withEnv(
                "FLINK_PROPERTIES",
                "jobmanager.rpc.address: jobmanager\n"
                    + "execution.attached: true\n"
                    + "openlineage.transport.url: http://openlineageclient:1080\n"
                    + "openlineage.transport.type: http\n")
            .withStartupTimeout(Duration.of(5, ChronoUnit.MINUTES))
            .dependsOn(Arrays.asList(generateEvents, openLineageClientMockContainer));

    taskManager =
        FlinkContainerUtils.makeFlinkTaskManagerContainer(network, Arrays.asList(jobManager));
    taskManager.start();

    await()
        .atMost(Duration.ofMinutes(5))
        .pollDelay(Duration.ofSeconds(5))
        .pollInterval(Duration.ofSeconds(1))
        .until(() -> FlinkContainerUtils.verifyJobManagerReachedCheckpointOrFinished(jobManager));

    verify("events/expected_flink_conf.json");
  }

  @Test
  @SneakyThrows
  void testJobTrackerStopsAfterJobIsExecuted() {
    runUntilNotRunning("io.openlineage.flink.FlinkStoppableApplication");

    await()
        .atMost(Duration.ofSeconds(10))
        .pollInterval(Duration.ofSeconds(2))
        .untilAsserted(
            () -> {
              HttpRequest[] requests =
                  mockServerClient.retrieveRecordedRequests(
                      request()
                          .withPath("/api/v1/lineage")
                          .withBody(
                              JsonBody.json("{\"job\":  {\"name\": \"flink-stoppable-job\"}}")));
              // last element has to be "COMPLETE"
              assertThat(
                      Arrays.stream(requests)
                          .map(request -> request.getBodyAsString())
                          .map(body -> body.contains("\"COMPLETE\"") ? "complete" : body)
                          .collect(Collectors.toList()))
                  .last()
                  .isEqualTo("complete");
            });
  }

  @SneakyThrows
  private JsonBody readJson(Path path) {
    return json(new String(readAllBytes(path)), MatchType.ONLY_MATCHING_FIELDS);
  }
}
