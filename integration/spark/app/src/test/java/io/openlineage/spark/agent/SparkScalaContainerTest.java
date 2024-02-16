/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent;

import static io.openlineage.spark.agent.SparkContainerProperties.CONTAINER_FIXTURES_JAR_PATH;
import static io.openlineage.spark.agent.SparkContainerProperties.CONTAINER_SPARK_CONF_DIR;
import static io.openlineage.spark.agent.SparkContainerProperties.CONTAINER_SPARK_JARS_DIR;
import static io.openlineage.spark.agent.SparkContainerProperties.HOST_ADDITIONAL_CONF_DIR;
import static io.openlineage.spark.agent.SparkContainerProperties.HOST_ADDITIONAL_JARS_DIR;
import static io.openlineage.spark.agent.SparkContainerProperties.HOST_LIB_DIR;
import static io.openlineage.spark.agent.SparkContainerProperties.HOST_SCALA_FIXTURES_JAR_PATH;
import static io.openlineage.spark.agent.SparkContainerProperties.SPARK_DOCKER_IMAGE;
import static io.openlineage.spark.agent.SparkContainerUtils.SPARK_DOCKER_CONTAINER_WAIT_MESSAGE;
import static io.openlineage.spark.agent.SparkContainerUtils.addSparkConfig;
import static io.openlineage.spark.agent.SparkContainerUtils.mountFiles;
import static io.openlineage.spark.agent.SparkContainerUtils.mountPath;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockserver.model.HttpRequest.request;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.RunEvent;
import io.openlineage.client.OpenLineageClientUtils;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
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
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.MockServerContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

/**
 * This class runs integration test for a Spark job written in scala. Having a Scala job allows us
 * to test `toDF`/`rdd` methods which are slightly different for Spark jobs written in Scala.
 *
 * <p>The integration test relies on bitnami/spark docker image. It requires `spark.version` to
 * specify which Spark version should be tested. It also requires `openlineage.spark.jar` system
 * property which is set in `build.gradle`. @See https://hub.docker.com/r/bitnami/spark/
 */
@Tag("integration-test")
@Testcontainers
@Slf4j
public class SparkScalaContainerTest {
  private static final Network network = Network.newNetwork();

  @Container
  private static final MockServerContainer openLineageClientMockContainer =
      SparkContainerUtils.makeMockServerContainer(network);

  private static GenericContainer<?> spark;
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
      if (spark != null) spark.stop();
    } catch (Exception e) {
      logger.error("Unable to shut down pyspark container", e);
    }
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

  @SneakyThrows
  private GenericContainer createSparkContainer(final String className) {
    List<String> commandParts = constructSparkSubmitCommand(className);
    String command = String.join(" ", commandParts);
    log.info("Container will be started with command: {}", command);

    GenericContainer container =
        new GenericContainer<>(DockerImageName.parse(SPARK_DOCKER_IMAGE))
            .withNetwork(network)
            .withNetworkAliases("spark")
            .withLogConsumer(SparkContainerUtils::consumeOutput)
            .waitingFor(Wait.forLogMessage(SPARK_DOCKER_CONTAINER_WAIT_MESSAGE, 1))
            .withStartupTimeout(Duration.of(2, ChronoUnit.MINUTES))
            .dependsOn(openLineageClientMockContainer)
            .withCommand(command);

    // mount the additional Jars
    mountPath(container, HOST_SCALA_FIXTURES_JAR_PATH, CONTAINER_FIXTURES_JAR_PATH);
    mountFiles(container, HOST_LIB_DIR, CONTAINER_SPARK_JARS_DIR);
    mountFiles(container, HOST_ADDITIONAL_JARS_DIR, CONTAINER_SPARK_JARS_DIR);
    mountFiles(container, HOST_ADDITIONAL_CONF_DIR, CONTAINER_SPARK_CONF_DIR);

    return container;
  }

  private List<String> constructSparkSubmitCommand(String className) {
    List<String> sparkSubmitCommand = new ArrayList<>();
    sparkSubmitCommand.add("./bin/spark-submit");
    sparkSubmitCommand.add("--master");
    sparkSubmitCommand.add("local");
    sparkSubmitCommand.add("--class");
    sparkSubmitCommand.add(className);

    addSparkConfig(sparkSubmitCommand, "spark.openlineage.transport.type=http");
    addSparkConfig(
        sparkSubmitCommand,
        "spark.openlineage.transport.url=http://openlineageclient:1080/api/v1/namespaces/scala-test");
    addSparkConfig(sparkSubmitCommand, "spark.openlineage.debugFacet=enabled");
    addSparkConfig(
        sparkSubmitCommand, "spark.extraListeners=" + OpenLineageSparkListener.class.getName());
    addSparkConfig(sparkSubmitCommand, "spark.sql.warehouse.dir=/tmp/warehouse");
    addSparkConfig(sparkSubmitCommand, "spark.sql.shuffle.partitions=1");
    addSparkConfig(
        sparkSubmitCommand, "spark.driver.extraJavaOptions=-Dderby.system.home=/tmp/derby");
    addSparkConfig(sparkSubmitCommand, "spark.jars.ivy=/tmp/.ivy2/");
    addSparkConfig(sparkSubmitCommand, "spark.openlineage.facets.disabled=");
    addSparkConfig(sparkSubmitCommand, "spark.ui.enabled=false");
    // Last, but not least, we add the path to the JAR
    sparkSubmitCommand.add(CONTAINER_FIXTURES_JAR_PATH.toString());

    return sparkSubmitCommand;
  }

  @Test
  void testScalaUnionRddToParquet() {
    spark = createSparkContainer("io.openlineage.spark.test.RddUnion");
    spark.start();

    await()
        .atMost(Duration.ofSeconds(10))
        .pollInterval(Duration.ofMillis(500))
        .untilAsserted(
            () -> {
              List<OpenLineage.RunEvent> events =
                  Arrays.stream(
                          mockServerClient.retrieveRecordedRequests(
                              request().withPath("/api/v1/lineage")))
                      .map(r -> r.getBodyAsString())
                      .map(event -> OpenLineageClientUtils.runEventFromJson(event))
                      .collect(Collectors.toList());
              RunEvent lastEvent = events.get(events.size() - 2);

              assertThat(events).isNotEmpty();
              assertThat(lastEvent.getOutputs().get(0))
                  .hasFieldOrPropertyWithValue("namespace", "file")
                  .hasFieldOrPropertyWithValue("name", "/tmp/scala-test/rdd_output");

              assertThat(lastEvent.getInputs().stream().map(d -> d.getName()))
                  .contains("/tmp/scala-test/rdd_input1", "/tmp/scala-test/rdd_input2");
            });
  }
}
