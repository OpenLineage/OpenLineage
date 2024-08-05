/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent;

import static io.openlineage.spark.agent.SparkContainerProperties.CONTAINER_FIXTURES_JAR_PATH;
import static io.openlineage.spark.agent.SparkContainerProperties.CONTAINER_SPARK_CONF_DIR;
import static io.openlineage.spark.agent.SparkContainerProperties.CONTAINER_SPARK_HOME_DIR;
import static io.openlineage.spark.agent.SparkContainerProperties.CONTAINER_SPARK_JARS_DIR;
import static io.openlineage.spark.agent.SparkContainerProperties.HOST_ADDITIONAL_CONF_DIR;
import static io.openlineage.spark.agent.SparkContainerProperties.HOST_ADDITIONAL_JARS_DIR;
import static io.openlineage.spark.agent.SparkContainerProperties.HOST_LIB_DIR;
import static io.openlineage.spark.agent.SparkContainerProperties.HOST_SCALA_FIXTURES_JAR_PATH;
import static io.openlineage.spark.agent.SparkContainerProperties.SCALA_BINARY_VERSION;
import static io.openlineage.spark.agent.SparkContainerProperties.SPARK_DOCKER_IMAGE;
import static io.openlineage.spark.agent.SparkContainerUtils.SPARK_DOCKER_CONTAINER_WAIT_MESSAGE;
import static io.openlineage.spark.agent.SparkContainerUtils.addSparkConfig;
import static io.openlineage.spark.agent.SparkContainerUtils.mountFiles;
import static io.openlineage.spark.agent.SparkContainerUtils.mountPath;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockserver.model.HttpRequest.request;
import static org.testcontainers.containers.Network.newNetwork;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.RunEvent;
import io.openlineage.client.OpenLineageClientUtils;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
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
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.mockserver.client.MockServerClient;
import org.mockserver.model.ClearType;
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
class SparkScalaContainerTest {
  private static final Network network = newNetwork();

  @Container
  private static final MockServerContainer openLineageClientMockContainer =
      SparkContainerUtils.makeMockServerContainer(network);

  private static GenericContainer<?> spark;
  private static MockServerClient mockServerClient;
  private static final Logger logger = LoggerFactory.getLogger(SparkContainerIntegrationTest.class);
  private static final String SPARK_3_OR_ABOVE = "^[3-9].*";
  private static final String SPARK_3_ONLY = "^3.*";
  private static final String SCALA_2_12 = "^2.12.*";
  private static final String SCALA_VERSION = "scala.binary.version";

  private static final String SPARK_VERSION = "spark.version";

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
  public void cleanup() {
    mockServerClient.clear(request("/api/v1/lineage"), ClearType.LOG);
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
    sparkSubmitCommand.add(Paths.get(System.getProperty("spark.home.dir")) + "/bin/spark-submit");
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
  @SuppressWarnings("PMD.JUnitTestsShouldIncludeAssert")
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

              assertThat(events).isNotEmpty();

              RunEvent lastEvent = events.get(events.size() - 2);
              assertThat(lastEvent.getOutputs().get(0))
                  .hasFieldOrPropertyWithValue("namespace", "file")
                  .hasFieldOrPropertyWithValue("name", "/tmp/scala-test/rdd_output");

              assertThat(lastEvent.getInputs().stream().map(d -> d.getName()))
                  .contains("/tmp/scala-test/rdd_input1", "/tmp/scala-test/rdd_input2");
            });
  }

  @Test
  @EnabledIfSystemProperty(named = SPARK_VERSION, matches = SPARK_3_OR_ABOVE)
  void testKafka2KafkaStreamingProducesInputAndOutputDatasets() throws IOException {
    final Network network = newNetwork();
    final String className = "io.openlineage.spark.streaming.Kafka2KafkaJob";
    final DockerImageName kafkaDockerImageName =
        DockerImageName.parse("docker.io/bitnami/kafka:3.4.1");

    GenericContainer zookeeperContainer =
        new GenericContainer(DockerImageName.parse("docker.io/bitnami/zookeeper:3.7"))
            .withEnv("ALLOW_ANONYMOUS_LOGIN", "yes")
            .withEnv("ZOO_AUTOPURGE_INTERVAL", "1")
            .withNetwork(network)
            .withNetworkAliases("zookeeper")
            .withExposedPorts(2181);

    GenericContainer kafkaContainer =
        new GenericContainer(kafkaDockerImageName)
            .withEnv("KAFKA_CFG_ZOOKEEPER_CONNECT", "zookeeper:2181")
            .withEnv(
                "KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP", "CLIENT:PLAINTEXT,EXTERNAL:PLAINTEXT")
            .withEnv("KAFKA_CFG_LISTENERS", "CLIENT://:9092,EXTERNAL://:9093")
            .withEnv(
                "KAFKA_CFG_ADVERTISED_LISTENERS",
                "CLIENT://kafka.broker.zero:9092,EXTERNAL://localhost:9093")
            .withEnv("KAFKA_CFG_INTER_BROKER_LISTENER_NAME", "CLIENT")
            .withEnv("ALLOW_PLAINTEXT_LISTENER", "yes")
            .withEnv("KAFKA_ZOOKEEPER_TLS_VERIFY_HOSTNAME", "false")
            .withEnv("KAFKA_CFG_AUTO_CREATE_TOPICS_ENABLE", "true")
            .withNetwork(network)
            .withNetworkAliases("kafka.broker.zero")
            .withExposedPorts(9092, 9093)
            .dependsOn(zookeeperContainer);

    GenericContainer kafkaInitContainer =
        new GenericContainer(kafkaDockerImageName)
            .withEnv("KAFKA_CFG_ZOOKEEPER_CONNECT", "zookeeper:2181")
            .withNetwork(network)
            .dependsOn(zookeeperContainer, kafkaContainer)
            .withCommand(
                "/bin/bash",
                "-c",
                "/opt/bitnami/kafka/bin/kafka-topics.sh --create --topic input-topic --bootstrap-server kafka.broker.zero:9092");

    zookeeperContainer.start();
    kafkaContainer.start();
    kafkaInitContainer.start();

    GenericContainer spark =
        new GenericContainer<>(DockerImageName.parse(SPARK_DOCKER_IMAGE))
            .dependsOn(kafkaContainer)
            .waitingFor(Wait.forLogMessage(SPARK_DOCKER_CONTAINER_WAIT_MESSAGE, 1))
            .withNetwork(network)
            .withLogConsumer(SparkContainerUtils::consumeOutput)
            .withStartupTimeout(Duration.ofMinutes(2));

    List<String> command = new ArrayList<>();
    command.add(CONTAINER_SPARK_HOME_DIR + "/bin/spark-submit");
    command.add("--master");
    command.add("local[*]");
    command.add("--class");
    command.add(className);
    command.add("--packages");
    command.add(System.getProperty("kafka.package.version"));

    addSparkConfig(command, "spark.driver.extraJavaOptions=-Dderby.system.home=/tmp/derby");
    addSparkConfig(command, "spark.extraListeners=" + OpenLineageSparkListener.class.getName());
    addSparkConfig(command, "spark.jars.ivy=/tmp/.ivy2/");
    addSparkConfig(command, "spark.openlineage.debugFacet=enabled");
    addSparkConfig(
        command, "spark.openlineage.facets.disabled=[schema;spark_unknown;spark.logicalPlan]");
    addSparkConfig(command, "spark.openlineage.transport.type=file");
    addSparkConfig(command, "spark.openlineage.transport.location=/tmp/events.log");
    addSparkConfig(command, "spark.sql.shuffle.partitions=1");
    addSparkConfig(command, "spark.sql.warehouse.dir=/tmp/warehouse");
    addSparkConfig(command, "spark.ui.enabled=false");
    command.add(CONTAINER_FIXTURES_JAR_PATH.toString());

    // mount the additional Jars
    mountPath(spark, HOST_SCALA_FIXTURES_JAR_PATH, CONTAINER_FIXTURES_JAR_PATH);
    mountFiles(spark, HOST_LIB_DIR, CONTAINER_SPARK_JARS_DIR);
    mountFiles(spark, HOST_ADDITIONAL_JARS_DIR, CONTAINER_SPARK_JARS_DIR);
    mountFiles(spark, HOST_ADDITIONAL_CONF_DIR, CONTAINER_SPARK_CONF_DIR);

    final String commandStr = String.join(" ", command);
    log.info("Command is {}", commandStr);

    spark.withCommand(commandStr);

    spark.start();

    Awaitility.await().atMost(Duration.ofSeconds(60)).until(() -> !spark.isRunning());

    kafkaInitContainer.stop();
    kafkaContainer.stop();
    zookeeperContainer.stop();

    File temporaryFile = File.createTempFile("events", ".log");

    spark.copyFileFromContainer("/tmp/events.log", temporaryFile.getPath());

    temporaryFile.deleteOnExit();

    List<RunEvent> events =
        Files.readAllLines(temporaryFile.toPath()).stream()
            .map(OpenLineageClientUtils::runEventFromJson)
            .collect(Collectors.toList());

    List<RunEvent> nonEmptyInputEvents =
        events.stream().filter(e -> !e.getInputs().isEmpty()).collect(Collectors.toList());

    assertThat(nonEmptyInputEvents).isNotEmpty();

    nonEmptyInputEvents.forEach(
        event -> {
          assertEquals(1, event.getInputs().size());
          assertEquals("input-topic", event.getInputs().get(0).getName());
          assertEquals("kafka://kafka.broker.zero:9092", event.getInputs().get(0).getNamespace());

          assertEquals(1, event.getOutputs().size());
          assertEquals("output-topic", event.getOutputs().get(0).getName());
          assertEquals("kafka://kafka.broker.zero:9092", event.getOutputs().get(0).getNamespace());
        });
  }

  @Test
  @EnabledIfSystemProperty(named = SPARK_VERSION, matches = SPARK_3_ONLY)
  @EnabledIfSystemProperty(named = SCALA_VERSION, matches = SCALA_2_12)
  void testReadingFromKinesis() throws IOException, InterruptedException {
    final String className = "io.openlineage.spark.streaming.KinesisReadJob";
    Network localstackNetwork = newNetwork();

    // latest left only because of issue with 3.6 version as 3.5, its planned to be fixed in 3.7
    // (end of august)
    GenericContainer localStack =
        new GenericContainer<>(DockerImageName.parse("localstack/localstack:latest"))
            .withNetwork(localstackNetwork)
            .withNetworkAliases("localstack")
            .withLogConsumer(SparkContainerUtils::consumeOutput)
            .withExposedPorts(4566)
            .withCommand();

    localStack.start();

    GenericContainer pythonContainer =
        new GenericContainer<>(DockerImageName.parse("python:3.11"))
            .withNetwork(localstackNetwork)
            .withNetworkAliases("python")
            .withCommand("sh", "-c", "tail -f /dev/null")
            .dependsOn(localStack)
            .withLogConsumer(SparkContainerUtils::consumeOutput);

    pythonContainer.start();

    pythonContainer.execInContainer("pip", "install", "awscli-local", "awscli");
    pythonContainer.execInContainer(
        "awslocal",
        "--endpoint-url=http://localstack:4566",
        "kinesis",
        "create-stream",
        "--stream-name",
        "events",
        "--shard-count",
        "1");
    pythonContainer.execInContainer(
        "awslocal",
        "--endpoint-url=http://localstack:4566",
        "kinesis",
        "put-record",
        "--stream-name",
        "events",
        "--partition-key",
        "1",
        "--data",
        "XzxkYXRhPl8x");

    pythonContainer.execInContainer(
        "awslocal",
        "--endpoint-url=http://localstack:4566",
        "kinesis",
        "put-record",
        "--stream-name",
        "events",
        "--partition-key",
        "1",
        "--data",
        "XzxkYXRhPl8x");

    GenericContainer spark =
        new GenericContainer<>(DockerImageName.parse(SPARK_DOCKER_IMAGE))
            .dependsOn(localStack)
            .waitingFor(Wait.forLogMessage(SPARK_DOCKER_CONTAINER_WAIT_MESSAGE, 1))
            .withNetwork(localstackNetwork)
            .withLogConsumer(SparkContainerUtils::consumeOutput)
            .withEnv("AWS_ACCESS_KEY_ID", "test")
            .withEnv("AWS_SECRET_ACCESS_KEY=", "test")
            .withStartupTimeout(Duration.ofMinutes(2));

    List<String> command = new ArrayList<>();
    command.add("./bin/spark-submit");
    command.add("--master");
    command.add("local[*]");
    command.add("--class");
    command.add(className);
    command.add("--jars");
    command.add(
        "https://awslabs-code-us-east-1.s3.amazonaws.com/spark-sql-kinesis-connector/spark-streaming-sql-kinesis-connector_"
            + SCALA_BINARY_VERSION
            + "-1.0.0.jar");

    addSparkConfig(command, "spark.driver.extraJavaOptions=-Dderby.system.home=/tmp/derby");
    addSparkConfig(command, "spark.extraListeners=" + OpenLineageSparkListener.class.getName());
    addSparkConfig(command, "spark.jars.ivy=/tmp/.ivy2/");
    addSparkConfig(command, "spark.openlineage.debugFacet=enabled");
    addSparkConfig(command, "spark.openlineage.facets.disabled=[spark_unknown]");
    addSparkConfig(command, "spark.openlineage.transport.type=file");
    addSparkConfig(command, "spark.openlineage.transport.location=/tmp/events.log");
    addSparkConfig(command, "spark.sql.shuffle.partitions=1");
    addSparkConfig(command, "spark.sql.warehouse.dir=/tmp/warehouse");
    addSparkConfig(command, "spark.ui.enabled=false");
    command.add(CONTAINER_FIXTURES_JAR_PATH.toString());

    // mount the additional Jars
    mountPath(spark, HOST_SCALA_FIXTURES_JAR_PATH, CONTAINER_FIXTURES_JAR_PATH);
    mountFiles(spark, HOST_LIB_DIR, CONTAINER_SPARK_JARS_DIR);
    mountFiles(spark, HOST_ADDITIONAL_JARS_DIR, CONTAINER_SPARK_JARS_DIR);
    mountFiles(spark, HOST_ADDITIONAL_CONF_DIR, CONTAINER_SPARK_CONF_DIR);

    final String commandStr = String.join(" ", command);
    log.info("Command is {}", commandStr);

    spark.withCommand(commandStr);
    spark.start();

    File temporaryFile = File.createTempFile("events", ".log");

    spark.copyFileFromContainer("/tmp/events.log", temporaryFile.getPath());

    List<RunEvent> events =
        Files.readAllLines(temporaryFile.toPath()).stream()
            .map(OpenLineageClientUtils::runEventFromJson)
            .collect(Collectors.toList());

    List<RunEvent> nonEmptyInputEvents =
        events.stream().filter(e -> !e.getInputs().isEmpty()).collect(Collectors.toList());

    assertFalse(nonEmptyInputEvents.isEmpty());

    List<SchemaUtils.SchemaRecord> expectedInputSchema =
        Arrays.asList(
            new SchemaUtils.SchemaRecord("data", "binary"),
            new SchemaUtils.SchemaRecord("streamName", "string"),
            new SchemaUtils.SchemaRecord("partitionKey", "string"),
            new SchemaUtils.SchemaRecord("sequenceNumber", "string"),
            new SchemaUtils.SchemaRecord("approximateArrivalTimestamp", "timestamp"));

    nonEmptyInputEvents.forEach(
        event -> {
          assertEquals(1, event.getInputs().size());
          assertEquals("events", event.getInputs().get(0).getName());
          assertEquals("kinesis://localstack:4566", event.getInputs().get(0).getNamespace());
          assertEquals(
              expectedInputSchema,
              SchemaUtils.mapToSchemaRecord(event.getInputs().get(0).getFacets().getSchema()));
        });
  }
}
