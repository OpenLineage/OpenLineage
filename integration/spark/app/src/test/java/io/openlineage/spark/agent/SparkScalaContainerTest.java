/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent;

import static io.openlineage.spark.agent.SparkContainerUtils.addSparkConfig;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockserver.model.HttpRequest.request;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.RunEvent;
import io.openlineage.client.OpenLineageClientUtils;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
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
import org.testcontainers.images.builder.ImageFromDockerfile;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

/**
 * This class runs integration test for a Spark job written in scala. All the other tests run python
 * spark scripts instead. Having a Scala job allows us to test `toDF`/`rdd` methods which are
 * slightly different for Spark jobs written in Scala.
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

  private GenericContainer createSparkContainer(String script) {
    String scalaVersion = scala.util.Properties.versionNumberString();
    GenericContainer<?> container;
    if (scalaVersion.startsWith("2.11") || scalaVersion.startsWith("2.12")) {
      container =
          new GenericContainer<>(
              DockerImageName.parse("bitnami/spark:" + System.getProperty("spark.version"))
                  .asCanonicalNameString());
    } else if (scalaVersion.startsWith("2.13")) {
      container =
          new GenericContainer<>(
              new ImageFromDockerfile()
                  .withFileFromPath("Dockerfile", Paths.get("../docker/scala213/Dockerfile"))
                  .withBuildArg("SPARK_VERSION", System.getProperty("spark.version")));
    } else {
      throw new RuntimeException(
          "Scala version not supported (only support 2.11, 2.12 2.13) " + scalaVersion);
    }
    return container
        .withNetwork(network)
        .withNetworkAliases("spark")
        .withFileSystemBind("src/test/resources/spark_scala_scripts", "/opt/spark_scala_scripts")
        .withFileSystemBind("src/test/resources/log4j.properties", "/opt/log4j.properties")
        .withFileSystemBind("build/libs", "/opt/libs")
        .withLogConsumer(SparkContainerUtils::consumeOutput)
        .waitingFor(Wait.forLogMessage(".*scala> :quit.*", 1))
        .withStartupTimeout(Duration.of(10, ChronoUnit.MINUTES))
        .dependsOn(openLineageClientMockContainer)
        .withReuse(true)
        .withCommand(
            sparkShellCommandForScript("/opt/spark_scala_scripts/" + script)
                .toArray(new String[] {}));
  }

  private List<String> sparkShellCommandForScript(String script) {
    List<String> command = new ArrayList<>();
    addSparkConfig(command, "spark.openlineage.transport.type=http");
    addSparkConfig(
        command,
        "spark.openlineage.transport.url=http://openlineageclient:1080/api/v1/namespaces/scala-test");
    addSparkConfig(command, "spark.openlineage.debugFacet=enabled");
    addSparkConfig(command, "spark.extraListeners=" + OpenLineageSparkListener.class.getName());
    addSparkConfig(command, "spark.sql.warehouse.dir=/tmp/warehouse");
    addSparkConfig(command, "spark.sql.shuffle.partitions=1");
    addSparkConfig(command, "spark.driver.extraJavaOptions=-Dderby.system.home=/tmp/derby");
    addSparkConfig(command, "spark.sql.warehouse.dir=/tmp/warehouse");
    addSparkConfig(command, "spark.jars.ivy=/tmp/.ivy2/");
    addSparkConfig(command, "spark.openlineage.facets.disabled=");
    addSparkConfig(
        command, "spark.driver.extraJavaOptions=-Dlog4j.configuration=/opt/log4j.properties");

    List<String> sparkShell =
        new ArrayList(Arrays.asList("./bin/spark-shell", "--master", "local", "-i", script));
    sparkShell.addAll(command);
    sparkShell.addAll(
        Arrays.asList("--jars", "/opt/libs/" + System.getProperty("openlineage.spark.jar")));

    log.info("Running spark-shell command: ", String.join(" ", sparkShell));

    return sparkShell;
  }

  @Test
  void testScalaUnionRddToParquet() {
    spark = createSparkContainer("rdd_union.scala");
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
              RunEvent lastEvent = events.get(events.size() - 1);

              assertThat(events).isNotEmpty();
              assertThat(lastEvent.getOutputs().get(0))
                  .hasFieldOrPropertyWithValue("namespace", "file")
                  .hasFieldOrPropertyWithValue("name", "/tmp/scala-test/rdd_output");

              assertThat(lastEvent.getInputs().stream().map(d -> d.getName()))
                  .contains("/tmp/scala-test/rdd_input1", "/tmp/scala-test/rdd_input2");
            });
  }
}
