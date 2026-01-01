/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent;

import static io.openlineage.spark.agent.SparkContainerProperties.CONTAINER_LOG4J2_PROPERTIES_PATH;
import static io.openlineage.spark.agent.SparkContainerProperties.CONTAINER_LOG4J_PROPERTIES_PATH;
import static io.openlineage.spark.agent.SparkContainerProperties.HOST_LIB_DIR;
import static io.openlineage.spark.agent.SparkContainerProperties.HOST_LOG4J2_PROPERTIES_PATH;
import static io.openlineage.spark.agent.SparkContainerProperties.HOST_LOG4J_PROPERTIES_PATH;
import static io.openlineage.spark.agent.SparkContainerProperties.HOST_RESOURCES_DIR;
import static io.openlineage.spark.agent.SparkContainerProperties.SPARK_DOCKER_IMAGE;
import static io.openlineage.spark.agent.SparkContainerUtils.SPARK_DOCKER_CONTAINER_WAIT_MESSAGE;
import static io.openlineage.spark.agent.SparkContainerUtils.mountFiles;
import static io.openlineage.spark.agent.SparkContainerUtils.mountPath;
import static io.openlineage.spark.agent.SparkTestUtils.SPARK_VERSION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockserver.model.HttpRequest.request;

import io.openlineage.client.OpenLineage.RunEvent;
import io.openlineage.client.OpenLineageClientUtils;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.mockserver.client.MockServerClient;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.MockServerContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Tag("integration-test")
@Testcontainers
@Slf4j
@EnabledIfSystemProperty(named = SPARK_VERSION, matches = "([34].*)") // Spark version >= 3.*
@SuppressWarnings("PMD.AvoidDuplicateLiterals")
class AwsDynamicFrameIntegrationTest {

  public static final String SPARK_DOCKER_IMAGE = "amazon/aws-glue-libs:5";

  private static final Network network = Network.newNetwork();

  @Container
  private static final MockServerContainer mockServerContainer =
      SparkContainerUtils.makeMockServerContainer(network);

  private static MockServerClient mockServerClient;

  @BeforeAll
  public static void setupMockServer() {
    mockServerClient =
        new MockServerClient(mockServerContainer.getHost(), mockServerContainer.getServerPort());

    mockServerClient
        .when(request("/api/v1/lineage"))
        .respond(org.mockserver.model.HttpResponse.response().withStatusCode(201));

    Awaitility.await().until(mockServerContainer::isRunning);
  }

  @AfterAll
  public static void tearDown() {
    mockServerContainer.stop();
    network.close();
  }

  @Test
  @SneakyThrows
  @EnabledIfSystemProperty(named = "scala.binary.version", matches = "2\\.12")
  void testDynamicFrame() {
    StringBuilder command = new StringBuilder("spark-submit");
    command
        .append(" --conf ")
        .append("spark.openlineage.transport.type=http")
        .append(" --conf ")
        .append("spark.openlineage.transport.url=http://openlineageclient:1080")
        .append(" --conf ")
        .append("spark.openlineage.facets.debug.disabled=false")
        .append(" --conf ")
        .append("spark.extraListeners=")
        .append(OpenLineageSparkListener.class.getName())
        .append(" --files ")
        .append(CONTAINER_LOG4J_PROPERTIES_PATH)
        .append(" /opt/spark_scripts/dynamic_frame.py")
        .append(" -Dlog4j.configuration=log4j.properties");

    log.info(command.toString());
    GenericContainer container =
        new GenericContainer<>(DockerImageName.parse(SPARK_DOCKER_IMAGE))
            .withNetwork(network)
            .withNetworkAliases("spark")
            .withLogConsumer(SparkContainerUtils::consumeOutput)
            .waitingFor(Wait.forLogMessage(SPARK_DOCKER_CONTAINER_WAIT_MESSAGE, 1))
            .dependsOn(mockServerContainer)
            .withCommand(command.toString());

    mountPath(container, HOST_RESOURCES_DIR.resolve("test_data"), Paths.get("/test_data"));
    mountFiles(
        container, HOST_RESOURCES_DIR.resolve("spark_scripts"), Paths.get("/opt/spark_scripts"));
    mountPath(container, HOST_LOG4J_PROPERTIES_PATH, CONTAINER_LOG4J_PROPERTIES_PATH);
    mountPath(container, HOST_LOG4J2_PROPERTIES_PATH, CONTAINER_LOG4J2_PROPERTIES_PATH);
    mountFiles(container, HOST_LIB_DIR, Paths.get("/opt/spark/jars/"));

    container.start();

    List<RunEvent> events =
        Arrays.stream(
                mockServerClient.retrieveRecordedRequests(request().withPath("/api/v1/lineage")))
            .map(r -> OpenLineageClientUtils.runEventFromJson(r.getBodyAsString()))
            .collect(Collectors.toList());

    assertThat(events.stream().flatMap(e -> e.getInputs().stream()).collect(Collectors.toList()))
        .isNotEmpty();
    assertThat(events.stream().flatMap(e -> e.getOutputs().stream()).collect(Collectors.toList()))
        .isNotEmpty();

    RunEvent insertEvent =
        events.stream()
            .filter(
                e ->
                    e.getJob().getName().contains("execute_insert_into_hadoop_fs_relation_command"))
            .filter(e -> RunEvent.EventType.COMPLETE.equals(e.getEventType()))
            .findFirst()
            .orElseThrow(() -> new AssertionError("No insert event found"));

    assertThat(insertEvent.getInputs()).hasSize(1);
    assertThat(insertEvent.getInputs().get(0).getName()).isEqualTo("/test_data");

    assertThat(insertEvent.getOutputs()).hasSize(1);
    assertThat(insertEvent.getOutputs().get(0).getName()).isEqualTo("/tmp/glue-test-job");
    assertThat(insertEvent.getOutputs().get(0).getFacets().getColumnLineage()).isNotNull();
  }
}
