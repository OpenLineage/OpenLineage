/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent;

import static io.openlineage.spark.agent.SparkContainerProperties.CONTAINER_LOG4J2_PROPERTIES_PATH;
import static io.openlineage.spark.agent.SparkContainerProperties.CONTAINER_LOG4J_PROPERTIES_PATH;
import static io.openlineage.spark.agent.SparkContainerProperties.CONTAINER_SPARK_HOME_DIR;
import static io.openlineage.spark.agent.SparkContainerProperties.CONTAINER_SPARK_JARS_DIR;
import static io.openlineage.spark.agent.SparkContainerProperties.HOST_ADDITIONAL_JARS_DIR;
import static io.openlineage.spark.agent.SparkContainerProperties.HOST_DEPENDENCIES_DIR;
import static io.openlineage.spark.agent.SparkContainerProperties.HOST_LIB_DIR;
import static io.openlineage.spark.agent.SparkContainerProperties.HOST_LOG4J2_PROPERTIES_PATH;
import static io.openlineage.spark.agent.SparkContainerProperties.HOST_LOG4J_PROPERTIES_PATH;
import static io.openlineage.spark.agent.SparkContainerProperties.HOST_RESOURCES_DIR;
import static io.openlineage.spark.agent.SparkContainerProperties.SPARK_DOCKER_IMAGE;

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
import lombok.extern.slf4j.Slf4j;
import org.mockserver.client.MockServerClient;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.MockServerContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

@Slf4j
public class SparkContainerUtils {
  public static final String SPARK_DOCKER_CONTAINER_WAIT_MESSAGE = ".*Shutdown hook called.*";

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
    PostgreSQLContainer container =
        new PostgreSQLContainer<>(DockerImageName.parse("postgres:13.4-bullseye"))
            .withNetwork(network)
            .withNetworkAliases("metastore")
            .withUsername("admin")
            .withPassword("password")
            .withDatabaseName("test")
            .withExposedPorts(5432);
    final Path basePath = Paths.get("src/test/resources/metastore_psql/").toAbsolutePath();
    mountPath(
        container,
        basePath.resolve("init-db.sh"),
        Paths.get("/docker-entrypoint-initdb.d/init-db.sh"));
    mountPath(
        container, basePath.resolve("create-databases.sql"), Paths.get("/create-databases.sql"));
    mountPath(
        container, basePath.resolve("metastore-2.3.0.sql"), Paths.get("/metastore-2.3.0.sql"));
    mountPath(
        container, basePath.resolve("metastore-3.1.0.sql"), Paths.get("/metastore-3.1.0.sql"));

    return container;
  }

  public static void mountPath(GenericContainer<?> container, Path sourcePath, Path targetPath) {
    if (log.isDebugEnabled()) {
      log.debug(
          "[image={}]: Mount volume '{}:{}'",
          container.getDockerImageName(),
          sourcePath,
          targetPath);
    }
    container.withFileSystemBind(sourcePath.toString(), targetPath.toString(), BindMode.READ_ONLY);
  }

  @SneakyThrows
  public static void mountFiles(GenericContainer<?> container, Path sourceDir, Path targetDir) {
    if (!Files.exists(sourceDir)) {
      log.warn("Source directory {} does not exist, skipping mount", sourceDir);
      return;
    }

    try (Stream<Path> files = Files.list(sourceDir)) {
      files.forEach(
          filePath -> {
            Path hostPath = filePath.toAbsolutePath();
            Path fileName = hostPath.getFileName();
            Path containerPath = targetDir.resolve(fileName);
            mountPath(container, hostPath, containerPath);
          });
    }
  }

  private static GenericContainer<?> makePysparkContainer(
      Network network,
      String waitMessage,
      MockServerContainer mockServerContainer,
      String... command) {
    GenericContainer container =
        new GenericContainer<>(DockerImageName.parse(SPARK_DOCKER_IMAGE))
            .withNetwork(network)
            .withNetworkAliases("spark")
            .withLogConsumer(SparkContainerUtils::consumeOutput)
            .waitingFor(Wait.forLogMessage(waitMessage, 1))
            .withStartupTimeout(Duration.of(10, ChronoUnit.MINUTES))
            .dependsOn(mockServerContainer)
            .withCommand(command);

    final Path buildDir = Paths.get(System.getProperty("build.dir")).toAbsolutePath();
    mountPath(container, buildDir.resolve("gcloud"), Paths.get("/opt/gcloud"));
    mountPath(container, HOST_RESOURCES_DIR.resolve("test_data"), Paths.get("/test_data"));
    mountFiles(
        container, HOST_RESOURCES_DIR.resolve("spark_scripts"), Paths.get("/opt/spark_scripts"));
    mountPath(container, buildDir.resolve("libs"), Paths.get("/opt/libs"));
    mountPath(container, HOST_DEPENDENCIES_DIR, Paths.get("/opt/dependencies"));
    mountPath(container, HOST_LOG4J_PROPERTIES_PATH, CONTAINER_LOG4J_PROPERTIES_PATH);
    mountPath(container, HOST_LOG4J2_PROPERTIES_PATH, CONTAINER_LOG4J2_PROPERTIES_PATH);
    mountFiles(container, HOST_ADDITIONAL_JARS_DIR, CONTAINER_SPARK_JARS_DIR);
    mountFiles(container, HOST_LIB_DIR, CONTAINER_SPARK_JARS_DIR);

    return container;
  }

  static GenericContainer<?> makeKafkaContainer(Network network) {
    return new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.0.0"))
        .withNetworkAliases("kafka-host")
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
        SPARK_DOCKER_CONTAINER_WAIT_MESSAGE,
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
    addSparkConfig(sparkConf, "spark.openlineage.facets.spark.logicalPlan.disabled=false");
    addSparkConfig(sparkConf, "spark.openlineage.facets.spark_unknown.disabled=false");
    addSparkConfig(sparkConf, "spark.openlineage.columnLineage.datasetLineageEnabled=false");
    addSparkConfig(
        sparkConf, "spark.openlineage.dataset.namespaceResolvers.kafka-cluster-prod.type=hostList");
    addSparkConfig(
        sparkConf,
        "spark.openlineage.dataset.namespaceResolvers.kafka-cluster-prod.hosts=[kafka-host;kafka-host-other]");

    String sparkSubmitPath = CONTAINER_SPARK_HOME_DIR + "/bin/spark-submit";
    List<String> sparkSubmit = new ArrayList(Arrays.asList(sparkSubmitPath, "--master", "local"));
    sparkSubmit.addAll(sparkConf);
    sparkSubmit.addAll(
        Arrays.asList(
            "--jars",
            "/opt/dependencies/spark-sql-kafka-*.jar"
                + ",/opt/dependencies/spark-bigquery-with-dependencies*.jar"
                + ",/opt/dependencies/gcs-connector-hadoop*.jar"
                + ",/opt/dependencies/google-http-client-*.jar"
                + ",/opt/dependencies/google-oauth-client-*.jar"
                + ",/opt/dependencies/kafka-*.jar"
                + ",/opt/dependencies/spark-token-provider-*.jar"
                + ",/opt/dependencies/commons-pool2-*.jar"));
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
