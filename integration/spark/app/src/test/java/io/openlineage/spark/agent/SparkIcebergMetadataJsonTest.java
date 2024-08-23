/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent;

import static io.openlineage.spark.agent.SparkContainerProperties.CONTAINER_FIXTURES_DIR;
import static io.openlineage.spark.agent.SparkContainerProperties.CONTAINER_FIXTURES_JAR_PATH;
import static io.openlineage.spark.agent.SparkContainerProperties.CONTAINER_SPARK_JARS_DIR;
import static io.openlineage.spark.agent.SparkContainerProperties.HOST_ADDITIONAL_JARS_DIR;
import static io.openlineage.spark.agent.SparkContainerProperties.HOST_LIB_DIR;
import static io.openlineage.spark.agent.SparkContainerProperties.HOST_SCALA_FIXTURES_JAR_PATH;
import static io.openlineage.spark.agent.SparkContainerProperties.SPARK_DOCKER_IMAGE;
import static io.openlineage.spark.agent.SparkContainerUtils.SPARK_DOCKER_CONTAINER_WAIT_MESSAGE;
import static io.openlineage.spark.agent.SparkTestsUtils.SPARK_VERSION;
import static org.testcontainers.containers.Network.newNetwork;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.model.AccessMode;
import com.github.dockerjava.api.model.Bind;
import com.github.dockerjava.api.model.HostConfig;
import com.github.dockerjava.api.model.Volume;
import io.openlineage.server.OpenLineage;
import io.openlineage.server.OpenLineage.RunEvent;
import io.openlineage.server.OpenLineage.RunEvent.EventType;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

/**
 * These tests differ from the {@link SparkIcebergIntegrationTest} in that the tests directly read
 * the {@code metadata.json} files that Iceberg produces instead of accessing the data via a {@link
 * org.apache.spark.sql.catalog.Catalog}.
 *
 * <p>These tests required that a change be performed in the
 */
@Slf4j
@Tag("integration-test")
@EnabledIfSystemProperty(named = SPARK_VERSION, matches = "^3\\.[2-5]\\.\\d*")
class SparkIcebergMetadataJsonTest {

  private static final Network NETWORK = newNetwork();
  private static final String SHARED_VOLUME_NAME = "spark-data";
  private static final Volume SHARED_VOLUME = new Volume("/tmp");
  private static final Path CONTAINER_TMP_DIR = Paths.get("/tmp");
  private static final Path CONTAINER_BASE_WAREHOUSE_DIR = CONTAINER_TMP_DIR.resolve("warehouse");
  private static final Path CONTAINER_SPARK_WAREHOUSE_DIR =
      CONTAINER_BASE_WAREHOUSE_DIR.resolve("spark");
  private static final Path CONTAINER_PUBLIC_WAREHOUSE_DIR =
      CONTAINER_BASE_WAREHOUSE_DIR.resolve("public");
  private static final Path CONTAINER_WORKSPACE_WAREHOUSE_DIR =
      CONTAINER_BASE_WAREHOUSE_DIR.resolve("workspace");
  private static final Path CONTAINER_LOG4J_PATH =
      CONTAINER_FIXTURES_DIR.resolve("log4j.properties");
  private static final String LOG4J_SYSTEM_PROPERTY =
      "-Dlog4j.configuration=file:" + CONTAINER_LOG4J_PATH;

  private static final ObjectMapper mapper =
      new ObjectMapper()
          .findAndRegisterModules()
          .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
  private static final ObjectReader reader = mapper.readerFor(OpenLineage.RunEvent.class);

  @BeforeAll
  static void setup() {
    createDockerVolumes();
    createSeedDataset();
  }

  @AfterAll
  static void tearDown() {
    NETWORK.close();
    deleteDockerVolumes();
  }

  static void createDockerVolumes() {
    DockerClient client = DockerClientFactory.instance().client();
    log.info("Creating docker volume: {}", SHARED_VOLUME_NAME);
    client.createVolumeCmd().withName(SHARED_VOLUME_NAME).exec();
  }

  static void deleteDockerVolumes() {
    DockerClient client = DockerClientFactory.instance().client();
    log.info("Deleting docker volume: {}", SHARED_VOLUME_NAME);
    client.removeVolumeCmd(SHARED_VOLUME_NAME).exec();
  }

  static void createSeedDataset() {
    log.info("Creating seed dataset");
    Map<String, String> props = new TreeMap<>(Comparator.naturalOrder());
    props.put("spark.app.name", "create-iceberg-seed-data");
    props.put("spark.app.master", "local[*]");
    props.put("spark.sql.shuffle.partitions", "1");
    props.put("spark.ui.enabled", "false");
    props.put("spark.sql.warehouse.dir", CONTAINER_SPARK_WAREHOUSE_DIR.toString());
    props.put("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog");
    props.put("spark.sql.catalog.spark_catalog.type", "hive");
    props.put("spark.sql.catalog.public", "org.apache.iceberg.spark.SparkCatalog");
    props.put("spark.sql.catalog.public.type", "hadoop");
    props.put("spark.sql.catalog.public.warehouse", CONTAINER_PUBLIC_WAREHOUSE_DIR.toString());
    props.put("spark.sql.defaultCatalog", "public");
    props.put("spark.driver.extraJavaOptions", LOG4J_SYSTEM_PROPERTY);

    List<String> commandParts =
        constructSparkSubmitCommand(
            "io.openlineage.spark.iceberg.CreateSeedDataJob", props, Collections.emptyList());

    GenericContainer<?> container = createSparkContainer(commandParts);
    container.start();
    container.close();
    log.info("Creating seed dataset complete");
  }

  static void copyFileToContainer(
      GenericContainer<?> container, Path hostPath, Path containerPath) {
    log.info("Copying {} to {}", hostPath, containerPath);
    Assertions.assertThat(hostPath).isRegularFile();
    container.withCopyFileToContainer(
        MountableFile.forHostPath(hostPath), containerPath.toString());
  }

  static void copyFilesToContainer(
      GenericContainer<?> container, Path hostDirectory, Path containerDirectory)
      throws IOException {
    try (Stream<Path> files = Files.list(hostDirectory)) {
      Iterator<Path> iterator = files.iterator();
      while (iterator.hasNext()) {
        Path hostPath = iterator.next();
        Path containerPath = containerDirectory.resolve(hostPath.getFileName().toString());
        copyFileToContainer(container, hostPath, containerPath);
      }
    }
  }

  @Test
  @EnabledIfSystemProperty(named = SPARK_VERSION, matches = "^3\\.5\\.\\d*")
  @SuppressWarnings("PMD") // assertions are handled in the nested method
  void readIcebergMetadataJsonOutsideConfiguredCatalogSpark35() {
    executeReadIcebergMetadataJsonOutsideConfiguredCatalog("append_data");
  }

  @Test
  @EnabledIfSystemProperty(named = SPARK_VERSION, matches = "^3\\.[2-4]\\.\\d*")
  @SuppressWarnings("PMD") // assertions are handled in the nested method
  void readIcebergMetadataJsonOutsideConfiguredCatalog() {
    executeReadIcebergMetadataJsonOutsideConfiguredCatalog("atomic_create_table_as_select");
  }

  private void executeReadIcebergMetadataJsonOutsideConfiguredCatalog(
      final String commandToFilterOn) {
    // This test is a "problem child"
    // The behaviour between Spark [3.2.w, 3.3.x, 3.4.y] follows one path, namely:
    // Iceberg will issue an "atomic_create_table_as_select" command for this
    // However, when Spark is 3.5.z, it first issues an "append_data" command, followed by the
    // "atomic_create_table_as_select" command. The problem is that in 3.5.z, the input table
    // is the table being written to, and according to the Iceberg catalog, it doesn't exist.
    // Fun times.
    final String testName = "read_iceberg_metadata_json_outside_configured_catalog";
    final String eventsJsonPath = String.format("/tmp/lineage/%s.ndjson", testName);
    Map<String, String> props = new TreeMap<>(Comparator.naturalOrder());
    props.put("spark.app.name", testName);
    props.put("spark.app.master", "local[*]");
    props.put("spark.driver.extraJavaOptions", LOG4J_SYSTEM_PROPERTY);
    props.put("spark.extraListeners", "io.openlineage.spark.agent.OpenLineageSparkListener");
    props.put("spark.openlineage.transport.type", "file");
    props.put("spark.openlineage.transport.location", eventsJsonPath);
    props.put("spark.openlineage.facets.columnLineage.disabled", "true");
    props.put("spark.openlineage.facets.dataSource.disabled", "true");
    props.put("spark.openlineage.facets.environment-properties.disabled", "true");
    props.put("spark.openlineage.facets.lifecycleStateChange.disabled", "true");
    props.put("spark.openlineage.facets.parent.disabled", "true");
    props.put("spark.openlineage.facets.processing_engine.disabled", "true");
    props.put("spark.openlineage.facets.schema.disabled", "true");
    props.put("spark.openlineage.facets.spark.logicalPlan.disabled", "true");
    props.put("spark.openlineage.facets.spark_properties.disabled", "true");
    props.put("spark.openlineage.facets.spark_unknown.disabled", "true");
    props.put("spark.openlineage.facets.storage.disabled", "true");
    props.put("spark.openlineage.facets.symlinks.disabled", "true");
    props.put("spark.openlineage.facets.version.disabled", "true");
    props.put("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog");
    props.put("spark.sql.catalog.spark_catalog.type", "hive");
    props.put("spark.sql.catalog.workspace", "org.apache.iceberg.spark.SparkCatalog");
    props.put("spark.sql.catalog.workspace.type", "hadoop");
    props.put(
        "spark.sql.catalog.workspace.warehouse", CONTAINER_WORKSPACE_WAREHOUSE_DIR.toString());
    props.put("spark.sql.defaultCatalog", "workspace");
    props.put("spark.sql.shuffle.partitions", "1");
    props.put("spark.sql.warehouse.dir", CONTAINER_SPARK_WAREHOUSE_DIR.toString());
    props.put("spark.ui.enabled", "false");

    List<String> command =
        constructSparkSubmitCommand(
            "io.openlineage.spark.iceberg.ReadIcebergMetadataJsonOutsideConfiguredCatalogJob",
            props,
            Collections.singletonList(
                CONTAINER_PUBLIC_WAREHOUSE_DIR
                    .resolve("openlineage_public/person/metadata/v1.metadata.json")
                    .toString()));

    GenericContainer<?> container = createSparkContainer(command);
    container.start();
    List<String> events =
        container.copyFileFromContainer(
            eventsJsonPath,
            inputStream -> {
              try (BufferedReader bufferedReader =
                  new BufferedReader(new InputStreamReader(inputStream))) {
                return bufferedReader.lines().collect(Collectors.toList());
              }
            });
    container.close();

    events.forEach(json -> log.info("{}: {}", testName, json));

    RunEvent runEvent =
        events.stream()
            .map(this::tryDeserialise)
            .filter(e -> e.getJob().getName().contains(commandToFilterOn))
            .filter(e -> e.getEventType().equals(EventType.COMPLETE))
            .findFirst()
            .orElseThrow(() -> new RuntimeException("Couldn't find COMPLETE event"));

    Assertions.assertThat(runEvent)
        .isNotNull()
        .returns(1, e -> e.getInputs().size())
        .returns(1, e -> e.getOutputs().size())
        .returns("file", e -> e.getInputs().get(0).getNamespace())
        .returns(
            CONTAINER_PUBLIC_WAREHOUSE_DIR
                .resolve("openlineage_public")
                .resolve("person")
                .toString(),
            e -> e.getInputs().get(0).getName())
        .returns("file", e -> e.getOutputs().get(0).getNamespace())
        .returns(
            CONTAINER_WORKSPACE_WAREHOUSE_DIR
                .resolve("openlineage_workspace")
                .resolve("person")
                .toString(),
            e -> e.getOutputs().get(0).getName());
  }

  @Test
  void readIcebergMetadataJsonWithoutAConfiguredIcebergCatalog() {
    final String testName = "read_iceberg_metadata_json_without_a_configured_iceberg_catalog";
    final String eventsJsonPath = String.format("/tmp/lineage/%s.ndjson", testName);
    Map<String, String> props = new TreeMap<>(Comparator.naturalOrder());
    props.put("spark.app.name", testName);
    props.put("spark.app.master", "local[*]");
    props.put("spark.driver.extraJavaOptions", LOG4J_SYSTEM_PROPERTY);
    props.put("spark.extraListeners", "io.openlineage.spark.agent.OpenLineageSparkListener");
    props.put("spark.openlineage.transport.type", "file");
    props.put("spark.openlineage.transport.location", eventsJsonPath);
    props.put("spark.openlineage.facets.columnLineage.disabled", "true");
    props.put("spark.openlineage.facets.dataSource.disabled", "true");
    props.put("spark.openlineage.facets.environment-properties.disabled", "true");
    props.put("spark.openlineage.facets.lifecycleStateChange.disabled", "true");
    props.put("spark.openlineage.facets.parent.disabled", "true");
    props.put("spark.openlineage.facets.processing_engine.disabled", "true");
    props.put("spark.openlineage.facets.schema.disabled", "true");
    props.put("spark.openlineage.facets.spark.logicalPlan.disabled", "true");
    props.put("spark.openlineage.facets.spark_properties.disabled", "true");
    props.put("spark.openlineage.facets.spark_unknown.disabled", "true");
    props.put("spark.openlineage.facets.storage.disabled", "true");
    props.put("spark.openlineage.facets.symlinks.disabled", "true");
    props.put("spark.openlineage.facets.version.disabled", "true");
    props.put("spark.sql.shuffle.partitions", "1");
    props.put("spark.sql.warehouse.dir", CONTAINER_SPARK_WAREHOUSE_DIR.toString());
    props.put("spark.ui.enabled", "false");

    List<String> command =
        constructSparkSubmitCommand(
            "io.openlineage.spark.iceberg.ReadIcebergMetadataJsonWithoutAConfiguredIcebergCatalog",
            props,
            Collections.singletonList(
                CONTAINER_PUBLIC_WAREHOUSE_DIR
                    .resolve("openlineage_public/person/metadata/v1.metadata.json")
                    .toString()));

    GenericContainer<?> container = createSparkContainer(command);
    container.start();
    List<String> events =
        container.copyFileFromContainer(
            eventsJsonPath,
            inputStream -> {
              try (BufferedReader bufferedReader =
                  new BufferedReader(new InputStreamReader(inputStream))) {
                return bufferedReader.lines().collect(Collectors.toList());
              }
            });
    container.close();

    events.forEach(json -> log.info("{}: {}", testName, json));

    RunEvent runEvent =
        events.stream()
            .map(this::tryDeserialise)
            .filter(e -> e.getJob().getName().contains("hadoop_fs_relation_command"))
            .filter(e -> e.getEventType().equals(EventType.COMPLETE))
            .findFirst()
            .orElseThrow(() -> new RuntimeException("Couldn't find COMPLETE event"));

    Assertions.assertThat(runEvent)
        .isNotNull()
        .returns(1, e -> e.getInputs().size())
        .returns(1, e -> e.getOutputs().size())
        .returns("file", e -> e.getInputs().get(0).getNamespace())
        .returns(
            CONTAINER_PUBLIC_WAREHOUSE_DIR
                .resolve("openlineage_public")
                .resolve("person")
                .toString(),
            e -> e.getInputs().get(0).getName())
        .returns("file", e -> e.getOutputs().get(0).getNamespace())
        .returns(
            CONTAINER_SPARK_WAREHOUSE_DIR.resolve("person").toString(),
            e -> e.getOutputs().get(0).getName());
  }

  @SneakyThrows
  private static GenericContainer<?> createSparkContainer(List<String> submitCommand) {
    String command = String.join(" ", submitCommand);
    log.info("Container will be started with command: {}", command);

    DockerImageName dockerImageName = DockerImageName.parse(SPARK_DOCKER_IMAGE);
    log.info("Docker Image Name: {}", dockerImageName);

    GenericContainer<?> container =
        new GenericContainer<>(dockerImageName)
            .withNetwork(NETWORK)
            .withNetworkAliases("spark")
            .withLogConsumer(SparkContainerUtils::consumeOutput)
            .waitingFor(Wait.forLogMessage(SPARK_DOCKER_CONTAINER_WAIT_MESSAGE, 1))
            .withStartupTimeout(Duration.ofSeconds(30L))
            .withCommand(command)
            .withCreateContainerCmdModifier(
                createContainerCmd ->
                    createContainerCmd.withHostConfig(
                        HostConfig.newHostConfig()
                            .withBinds(
                                new Bind(SHARED_VOLUME_NAME, SHARED_VOLUME, AccessMode.rw))));

    copyFileToContainer(container, HOST_SCALA_FIXTURES_JAR_PATH, CONTAINER_FIXTURES_JAR_PATH);
    copyFilesToContainer(container, HOST_LIB_DIR, CONTAINER_SPARK_JARS_DIR);
    copyFilesToContainer(container, HOST_ADDITIONAL_JARS_DIR, CONTAINER_SPARK_JARS_DIR);
    return container;
  }

  private static List<String> constructSparkSubmitCommand(
      String className, Map<String, String> props, List<String> applicationArgs) {
    List<String> commandParts = new ArrayList<>();
    String sparkSubmitBinPath =
        Paths.get(System.getProperty("spark.home.dir")) + "/bin/spark-submit";
    commandParts.add(sparkSubmitBinPath);
    for (Entry<String, String> entry : props.entrySet()) {
      commandParts.add("--conf");
      commandParts.add(String.format("%s=%s", entry.getKey(), entry.getValue()));
    }
    commandParts.add("--class");
    commandParts.add(className);
    commandParts.add(SparkContainerProperties.CONTAINER_FIXTURES_JAR_PATH.toString());
    commandParts.addAll(applicationArgs);
    return commandParts;
  }

  private OpenLineage.RunEvent tryDeserialise(String json) throws UncheckedIOException {
    try {
      return reader.readValue(json);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
