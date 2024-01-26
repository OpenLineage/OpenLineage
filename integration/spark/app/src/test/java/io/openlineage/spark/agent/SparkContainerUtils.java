/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;
import lombok.SneakyThrows;
import org.mockserver.client.MockServerClient;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.MockServerContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

public class SparkContainerUtils {
  private static final String OPENLINEAGE_SPARK_AGENT_JAR =
      System.getProperty("openlineage.spark.agent.jar");
  private static final Path HOST_OPENLINEAGE_SPARK_AGENT_JAR_PATH =
      Paths.get(System.getProperty("openlineage.spark.agent.jar.path"));
  private static final Path HOST_ADDITIONAL_JARS_DIR =
      Paths.get(System.getProperty("openlineage.spark.agent.additional.jars.dir")).toAbsolutePath();
  private static final Path HOST_TEST_DATA_DIR =
      Paths.get("src/test/resources/test_data").toAbsolutePath();
  private static final Path HOST_TEST_FIXTURES_DIR =
      Paths.get("src/test/resources/spark_scripts").toAbsolutePath();
  private static final Path HOST_LOG4J_PROPERTIES_PATH =
      Paths.get("src/test/resources/container/spark/log4j.properties").toAbsolutePath();
  private static final Path HOST_LOG4J2_PROPERTIES_PATH =
      Paths.get("src/test/resources/container/spark/log4j2.properties").toAbsolutePath();
  private static final Path CONTAINER_SPARK_HOME_DIR = Paths.get("/opt/bitnami/spark");
  private static final Path CONTAINER_SPARK_JARS_DIR = CONTAINER_SPARK_HOME_DIR.resolve("jars");
  private static final Path CONTAINER_OPEN_LINEAGE_JAR_PATH =
      CONTAINER_SPARK_JARS_DIR.resolve(OPENLINEAGE_SPARK_AGENT_JAR);
  private static final Path CONTAINER_SPARK_WORK_DIR = CONTAINER_SPARK_HOME_DIR.resolve("work");
  private static final Path CONTAINER_TEST_DATA_DIR = Paths.get("/test_data");
  private static final Path CONTAINER_TEST_FIXTURES_DIR = Paths.get("/opt/spark_scripts");
  private static final Path CONTAINER_LOG4J_PATH =
      CONTAINER_SPARK_HOME_DIR.resolve("conf/log4j.properties");
  private static final Path CONTAINER_LOG4J2_PROPERTIES_PATH =
      CONTAINER_SPARK_HOME_DIR.resolve("conf/log4j2.properties");

  public static final DockerImageName MOCKSERVER_IMAGE =
      DockerImageName.parse("mockserver/mockserver")
          .withTag("mockserver-" + MockServerClient.class.getPackage().getImplementationVersion());

  static MockServerContainer makeMockServerContainer(Network network) {
    return new MockServerContainer(MOCKSERVER_IMAGE)
        .withNetwork(network)
        .withNetworkAliases("openlineageclient")
        .withStartupTimeout(Duration.of(2, ChronoUnit.MINUTES));
  }

  static PostgreSQLContainer<?> makeMetastoreContainer(Network network) {
    String basePath = "src/test/resources/metastore_psql/";
    return new PostgreSQLContainer<>(DockerImageName.parse("postgres:13.4-bullseye"))
        .withNetwork(network)
        .withNetworkAliases("metastore")
        .withUsername("admin")
        .withPassword("password")
        .withDatabaseName("test")
        .withFileSystemBind(basePath + "init-db.sh", "/docker-entrypoint-initdb.d/init-db.sh")
        .withFileSystemBind(basePath + "create-databases.sql", "/create-databases.sql")
        .withFileSystemBind(basePath + "metastore-2.3.0.sql", "/metastore-2.3.0.sql")
        .withFileSystemBind(basePath + "metastore-3.1.0.sql", "/metastore-3.1.0.sql")
        .withExposedPorts(5432);
  }

  @SneakyThrows
  private static GenericContainer<?> makePysparkContainer(
      Network network,
      String waitMessage,
      MockServerContainer mockServerContainer,
      String... command) {
    GenericContainer container =
        new GenericContainer<>(DockerImageName.parse(System.getProperty("docker.image.name")))
            .withNetwork(network)
            .withNetworkAliases("spark")
            .withFileSystemBind("build/gcloud", "/opt/gcloud", BindMode.READ_ONLY)
            .withFileSystemBind(
                HOST_TEST_DATA_DIR.toString(),
                CONTAINER_TEST_DATA_DIR.toString(),
                BindMode.READ_ONLY)
            .withFileSystemBind(
                HOST_TEST_FIXTURES_DIR.toString(),
                CONTAINER_TEST_FIXTURES_DIR.toString(),
                BindMode.READ_ONLY)
            .withFileSystemBind(
                HOST_OPENLINEAGE_SPARK_AGENT_JAR_PATH.toString(),
                CONTAINER_OPEN_LINEAGE_JAR_PATH.toString(),
                BindMode.READ_ONLY)
            .withFileSystemBind(
                HOST_LOG4J_PROPERTIES_PATH.toString(),
                CONTAINER_LOG4J_PATH.toString(),
                BindMode.READ_ONLY)
            .withFileSystemBind(
                HOST_LOG4J2_PROPERTIES_PATH.toString(),
                CONTAINER_LOG4J2_PROPERTIES_PATH.toString(),
                BindMode.READ_ONLY)
            .withLogConsumer(SparkContainerUtils::consumeOutput)
            .waitingFor(Wait.forLogMessage(waitMessage, 1))
            .withStartupTimeout(Duration.of(10, ChronoUnit.MINUTES))
            .dependsOn(mockServerContainer)
            .withReuse(true)
            .withCommand(command);

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

  static GenericContainer<?> makeKafkaContainer(Network network) {
    return new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.0.0"))
        .withNetworkAliases("kafka")
        .withNetwork(network);
  }

  static GenericContainer<?> makePysparkContainerWithDefaultConf(
      Network network,
      MockServerContainer mockServerContainer,
      String namespace,
      List<String> urlParams,
      List<String> sparkConfigParams,
      String... command) {
    return makePysparkContainerWithDefaultConf(
        network,
        "http://openlineageclient:1080",
        ".*ShutdownHookManager: Shutdown hook called.*",
        mockServerContainer,
        namespace,
        urlParams,
        sparkConfigParams,
        command);
  }

  static GenericContainer<?> makePysparkContainerWithDefaultConf(
      Network network,
      MockServerContainer mockServerContainer,
      String namespace,
      String... command) {
    return makePysparkContainerWithDefaultConf(
        network,
        mockServerContainer,
        namespace,
        Collections.emptyList(),
        Collections.emptyList(),
        command);
  }

  static GenericContainer<?> makePysparkContainerWithDefaultConf(
      Network network,
      String openlineageUrl,
      String waitMessage,
      MockServerContainer mockServerContainer,
      String namespace,
      List<String> urlParams,
      List<String> sparkConfigParams,
      String... command) {
    String paramString = "";
    if (!urlParams.isEmpty()) {
      paramString = "?" + String.join("&", urlParams);
    }

    List<String> sparkConf = new ArrayList<>();
    sparkConfigParams.forEach(param -> addSparkConfig(sparkConf, param));
    addSparkConfig(sparkConf, "spark.openlineage.transport.type=http");
    addSparkConfig(
        sparkConf,
        "spark.openlineage.transport.url="
            + openlineageUrl
            + "/api/v1/namespaces/"
            + namespace
            + paramString);
    addSparkConfig(sparkConf, "spark.extraListeners=" + OpenLineageSparkListener.class.getName());
    addSparkConfig(sparkConf, "spark.sql.warehouse.dir=/tmp/warehouse");
    addSparkConfig(sparkConf, "spark.sql.shuffle.partitions=1");
    addSparkConfig(sparkConf, "spark.driver.extraJavaOptions=-Dderby.system.home=/tmp/derby");
    addSparkConfig(sparkConf, "spark.jars.ivy=/tmp/.ivy2/");
    addSparkConfig(sparkConf, "spark.openlineage.facets.disabled=");
    addSparkConfig(sparkConf, "spark.ui.enabled=false");

    List<String> sparkSubmit =
        new ArrayList<>(Arrays.asList("spark-submit", "--master", "local"));
    sparkSubmit.addAll(sparkConf);
    sparkSubmit.addAll(Arrays.asList(command));
    return makePysparkContainer(
        network, waitMessage, mockServerContainer, sparkSubmit.toArray(new String[0]));
  }

  public static void addSparkConfig(List<String> command, String value) {
    command.add("--conf");
    command.add(value);
  }

  static void runPysparkContainerWithDefaultConf(
      Network network,
      MockServerContainer mockServerContainer,
      String namespace,
      String pysparkFile) {
    runPysparkContainerWithDefaultConf(
        network,
        mockServerContainer,
        namespace,
        Collections.emptyList(),
        Collections.emptyList(),
        pysparkFile);
  }

  static void runPysparkContainerWithDefaultConf(
      Network network,
      MockServerContainer mockServerContainer,
      String namespace,
      List<String> urlParams,
      List<String> sparkConfigParams,
      String pysparkFile) {
    makePysparkContainerWithDefaultConf(
            network,
            mockServerContainer,
            namespace,
            urlParams,
            sparkConfigParams,
            "/opt/spark_scripts/" + pysparkFile)
        .start();
  }

  @SuppressWarnings("PMD")
  static void consumeOutput(org.testcontainers.containers.output.OutputFrame of) {
    try {
      switch (of.getType()) {
        case STDOUT:
          System.out.write(of.getBytes());
          break;
        case STDERR:
          System.err.write(of.getBytes());
          break;
        case END:
          System.out.println(of.getUtf8String());
          break;
      }
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }
}
