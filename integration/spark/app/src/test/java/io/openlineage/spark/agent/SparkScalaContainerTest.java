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
import io.openlineage.client.OpenLineage.InputDataset;
import io.openlineage.client.OpenLineage.RunEvent;
import io.openlineage.client.OpenLineageClientUtils;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockserver.client.MockServerClient;
import org.testcontainers.containers.BindMode;
import org.mockserver.model.HttpRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.MockServerContainer;
import org.testcontainers.containers.Network;
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
  private static final String PROJECT_VERSION = System.getProperty("project.version");
  private static final String OPENLINEAGE_SPARK_JAR_NAME =
      System.getProperty("openlineage.spark.agent.jar");
  private static final String SCALA_BINARY_VERSION = System.getProperty("scala.binary.version");
  private static final String TEST_FIXTURES_JAR_NAME =
      String.format(
          "openlineage-spark-scala-fixtures_%s-%s.jar", SCALA_BINARY_VERSION, PROJECT_VERSION);
  private static final Path CONTAINER_SPARK_HOME = Paths.get("/opt/bitnami/spark");
  private static final Path CONTAINER_WORK_DIR = Paths.get("/opt/bitnami/spark/work");
  private static final Path CONTAINER_IVY_DIR = CONTAINER_WORK_DIR.resolve(".ivy2");
  private static final Path CONTAINER_SPARK_CONF_DIR = CONTAINER_SPARK_HOME.resolve("conf");
  private static final Path CONTAINER_LOG4J_PATH =
      CONTAINER_SPARK_CONF_DIR.resolve("log4j.properties");
  private static final Path CONTAINER_SPARK_JARS_DIR = CONTAINER_SPARK_HOME.resolve("jars");
  private static final Path CONTAINER_OPENLINEAGE_SPARK_JAR_PATH =
      CONTAINER_SPARK_JARS_DIR.resolve(OPENLINEAGE_SPARK_JAR_NAME);
  private static final Path CONTAINER_SPARK_WAREHOUSE_DIR = CONTAINER_WORK_DIR.resolve("warehouse");
  private static final Path CONTAINER_TEST_FIXTURES_JAR_PATH =
      CONTAINER_WORK_DIR.resolve(TEST_FIXTURES_JAR_NAME);
  private static final Path HOST_ADDITIONAL_JARS_DIR =
      Paths.get(System.getProperty("openlineage.spark.agent.additional.jars.dir")).toAbsolutePath();
  private static final Path HOST_LOG4J_PATH =
      Paths.get("src/test/resources/container/spark/log4j.properties").toAbsolutePath();
  private static final Path HOST_OPENLINEAGE_SPARK_JAR_PATH =
      Paths.get(System.getProperty("openlineage.spark.agent.jar.path")).toAbsolutePath();
  private static final Path HOST_FIXTURES_DIR =
      Paths.get(System.getProperty("openlineage.spark.agent.fixtures.dir")).toAbsolutePath();
  private static final Path HOST_TEST_FIXTURES_JAR_PATH =
      HOST_FIXTURES_DIR.resolve(TEST_FIXTURES_JAR_NAME);

  private static final Network network = Network.newNetwork();

  @Container
  private static final MockServerContainer openLineageClientMockContainer =
      SparkContainerUtils.makeMockServerContainer(network);

  private static GenericContainer<?> spark;
  private static MockServerClient mockServerClient;

  @BeforeAll
  public static void setup() {
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
  public void cleanupSpark() {
    mockServerClient.reset();
    try {
      if (spark != null) spark.stop();
    } catch (Exception e) {
      log.error("Unable to shut down pyspark container", e);
    }
  }

  @AfterAll
  public static void tearDown() {
    try {
      openLineageClientMockContainer.stop();
    } catch (Exception e) {
      log.error("Unable to shut down openlineage client container", e);
    }
    network.close();
  }

  @SneakyThrows
  private GenericContainer createSparkContainer() {
    DockerImageName dockerImageName =
        DockerImageName.parse(System.getProperty("docker.image.name"));
    GenericContainer container =
        new GenericContainer<>(dockerImageName)
            .withNetwork(network)
            .withNetworkAliases("spark")
            // Here, we upload the `openlineage-spark` jar directly into the '${SPARK_HOME}/jars'
            // directory
            // This means we don't need to configure the command line to include the jar in the
            // classpath
            // during job submission.
            .withFileSystemBind(
                HOST_OPENLINEAGE_SPARK_JAR_PATH.toAbsolutePath().toString(),
                CONTAINER_OPENLINEAGE_SPARK_JAR_PATH.toString(),
                BindMode.READ_ONLY)
            // This is the jar that contains the test fixtures, namely the Scala job(s) that we need
            // to execute
            // This replaced the old 'rdd_union.scala' file we used, because something changed in
            // Scala 2.13's
            // REPL. The '-i' option no longer caused the file to be loaded.
            .withFileSystemBind(
                HOST_TEST_FIXTURES_JAR_PATH.toAbsolutePath().toString(),
                CONTAINER_TEST_FIXTURES_JAR_PATH.toString(),
                BindMode.READ_ONLY)
            .withFileSystemBind(
                HOST_LOG4J_PATH.toAbsolutePath().toString(),
                CONTAINER_LOG4J_PATH.toString(),
                BindMode.READ_ONLY)
            .withLogConsumer(SparkContainerUtils::consumeOutput)
            .withStartupTimeout(Duration.of(10, ChronoUnit.MINUTES))
            .dependsOn(openLineageClientMockContainer)
            .withReuse(false)
            .withCommand(sparkShellCommandForScript().toArray(new String[] {}));

    try (Stream<Path> files = Files.list(HOST_ADDITIONAL_JARS_DIR)) {
      files
          .map(Path::toAbsolutePath)
          .forEach(
              path -> {
                Path fileName = path.getFileName();
                container.withFileSystemBind(
                    path.toAbsolutePath().toString(),
                    CONTAINER_SPARK_JARS_DIR.resolve(fileName).toString(),
                    BindMode.READ_ONLY);
              });
    }

    return container;
  }

  @SneakyThrows
  private List<String> sparkShellCommandForScript() {
    List<String> command = new ArrayList<>();
    command.add("spark-submit");
    command.add("--class");
    command.add("io.openlineage.spark.test.RddUnion");
    command.add("--master");
    command.add("local");
    addSparkConfig(command, "spark.openlineage.transport.type=http");
    addSparkConfig(
        command, "spark.openlineage.transport.url=http://openlineageclient:1080/api/v1/lineage");
    addSparkConfig(command, "spark.openlineage.debugFacet=enabled");
    addSparkConfig(command, "spark.extraListeners=" + OpenLineageSparkListener.class.getName());
    addSparkConfig(command, "spark.sql.warehouse.dir=" + CONTAINER_SPARK_WAREHOUSE_DIR);
    addSparkConfig(command, "spark.sql.shuffle.partitions=1");
    addSparkConfig(command, "spark.jars.ivy=" + CONTAINER_IVY_DIR);
    addSparkConfig(command, "spark.openlineage.facets.disabled=");
    addSparkConfig(command, "spark.ui.enabled=false");
    command.add(CONTAINER_TEST_FIXTURES_JAR_PATH.toString());

    String commandString = String.join(" ", command);
    if (log.isDebugEnabled()) {
      log.debug("Command to be executed is: {}", commandString);
    }

    return command;
  }

  @SneakyThrows
  @Test
  void testScalaUnionRddToParquet() {
    spark = createSparkContainer();
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
                      .map(OpenLineageClientUtils::runEventFromJson)
                      .collect(Collectors.toList());
              assertThat(events).isNotEmpty();

              RunEvent lastEvent = events.get(events.size() - 1);
              assertThat(lastEvent.getOutputs().get(0))
                  .hasFieldOrPropertyWithValue("namespace", "file")
                  .hasFieldOrPropertyWithValue("name", "/tmp/scala-test/rdd_output");

              assertThat(lastEvent.getInputs().stream().map(d -> d.getName()))
                  .contains("/tmp/scala-test/rdd_input1", "/tmp/scala-test/rdd_input2");
            });
  }
}
