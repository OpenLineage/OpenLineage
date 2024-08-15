/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent;

import static io.openlineage.spark.agent.SparkContainerProperties.CONTAINER_FIXTURES_DIR;
import static io.openlineage.spark.agent.SparkContainerProperties.CONTAINER_FIXTURES_JAR_PATH;
import static io.openlineage.spark.agent.SparkContainerProperties.CONTAINER_SPARK_JARS_DIR;
import static io.openlineage.spark.agent.SparkContainerProperties.HOST_LIB_DIR;
import static io.openlineage.spark.agent.SparkContainerProperties.HOST_SCALA_FIXTURES_JAR_PATH;
import static io.openlineage.spark.agent.SparkContainerProperties.SPARK_DOCKER_IMAGE;
import static io.openlineage.spark.agent.SparkContainerUtils.SPARK_DOCKER_CONTAINER_WAIT_MESSAGE;
import static io.openlineage.spark.agent.SparkTestsUtils.SPARK_3_OR_ABOVE;
import static io.openlineage.spark.agent.SparkTestsUtils.SPARK_VERSION;
import static org.testcontainers.containers.Network.newNetwork;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.google.common.base.CaseFormat;
import io.openlineage.server.OpenLineage;
import io.openlineage.server.OpenLineage.RunEvent;
import io.openlineage.server.OpenLineage.RunEvent.EventType;
import io.openlineage.spark.agent.util.OpenLineageHttpHandler;
import io.openlineage.spark.agent.util.StatefulHttpServer;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

/**
 * These tests differ from the {@link SparkIcebergIntegrationTest} in that the tests directly read
 * the {@code metadata.json} files that Iceberg produces instead of accessing the data via a {@link
 * org.apache.spark.sql.catalog.Catalog}.
 *
 * <p>These tests required that a change be performed in the
 */
@Slf4j
@Tag("integration-test")
@EnabledIfSystemProperty(named = SPARK_VERSION, matches = SPARK_3_OR_ABOVE)
class SparkIcebergMetadataJsonTest {
  private static final Network network = newNetwork();
  private static final String NORMALIZED_CLASS_NAME =
      CaseFormat.UPPER_CAMEL.to(
          CaseFormat.LOWER_UNDERSCORE, SparkIcebergMetadataJsonTest.class.getSimpleName());

  private static final boolean IS_CI = System.getenv("CI") != null;
  private static final String TEST_OUT_DIR = System.getProperty("test.output.dir");

  private static final Path HOST_TEST_OUTPUT_DIR = Paths.get(IS_CI ? "/tmp" : TEST_OUT_DIR);
  private static final Path HOST_TEST_CLASS_OUTPUT_DIR =
      HOST_TEST_OUTPUT_DIR.resolve(NORMALIZED_CLASS_NAME).toAbsolutePath();

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
  private StatefulHttpServer server;

  @BeforeAll
  static void setup() throws IOException {
    clearTestOutputDir();
    createSeedDataset();
  }

  @BeforeEach
  void beforeEach() throws IOException {
    server = StatefulHttpServer.create("/api/lineage", new OpenLineageHttpHandler());
    server.start();
  }

  @AfterEach
  void afterEach() throws IOException {
    server.close();
    server = null; // NOPMD
  }

  @AfterAll
  static void tearDown() {
    network.close();
  }

  static void clearTestOutputDir() throws IOException {
    if (Files.exists(HOST_TEST_CLASS_OUTPUT_DIR)) {
      try (Stream<Path> stream = Files.walk(HOST_TEST_CLASS_OUTPUT_DIR)) {
        List<Path> paths = stream.sorted(Comparator.reverseOrder()).collect(Collectors.toList());
        for (Path path : paths) {
          Files.deleteIfExists(path);
        }
      }
    }
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

    List<String> command =
        constructSparkSubmitCommand(
            "io.openlineage.spark.iceberg.CreateSeedDataJob", props, Collections.emptyList());

    GenericContainer<?> container =
        createSparkContainer(
            command,
            c -> {
              c.withFileSystemBind(
                  HOST_TEST_CLASS_OUTPUT_DIR.toString(),
                  CONTAINER_TMP_DIR.toString(),
                  BindMode.READ_WRITE);
              c.withStartupTimeout(Duration.ofMinutes(1L));
            });
    container.start();
    container.stop();
    container.close();
    log.info("Creating seed dataset complete");
  }

  @Test
  void readIcebergMetadataJsonOutsideConfiguredCatalog() {
    final String testName = "read_iceberg_metadata_json_outside_configured_catalog";
    Map<String, String> props = new TreeMap<>(Comparator.naturalOrder());
    props.put("spark.app.name", testName);
    props.put("spark.app.master", "local[*]");
    props.put("spark.driver.extraJavaOptions", LOG4J_SYSTEM_PROPERTY);
    props.put("spark.extraListeners", "io.openlineage.spark.agent.OpenLineageSparkListener");
    props.put("spark.openlineage.transport.type", "http");
    props.put(
        "spark.openlineage.transport.url",
        String.format("http://host.docker.internal:%d", server.getPort()));
    props.put("spark.openlineage.transport.endpoint", "/api/lineage");
    props.put("spark.openlineage.facets.spark.logicalPlan.disabled", "true");
    props.put("spark.openlineage.facets.spark_unknown.disabled", "true");
    props.put("spark.openlineage.facets.debug.disabled", "true");
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

    GenericContainer<?> container =
        createSparkContainer(
            command,
            c -> {
              c.withFileSystemBind(
                  HOST_TEST_CLASS_OUTPUT_DIR.toString(),
                  CONTAINER_TMP_DIR.toString(),
                  BindMode.READ_WRITE);
              c.withStartupTimeout(Duration.ofSeconds(30));
            });
    container.start();
    container.stop();
    container.close();

    List<String> events = server.events();

    RunEvent runEvent =
        events.stream()
            .map(this::tryDeserialise)
            .filter(e -> e.getJob().getName().contains("atomic_create_table_as_select"))
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
    Map<String, String> props = new TreeMap<>(Comparator.naturalOrder());
    props.put("spark.app.name", testName);
    props.put("spark.app.master", "local[*]");
    props.put("spark.driver.extraJavaOptions", LOG4J_SYSTEM_PROPERTY);
    props.put("spark.extraListeners", "io.openlineage.spark.agent.OpenLineageSparkListener");
    props.put("spark.openlineage.transport.type", "http");
    props.put(
        "spark.openlineage.transport.url",
        String.format("http://host.docker.internal:%d", server.getPort()));
    props.put("spark.openlineage.transport.endpoint", "/api/lineage");
    props.put("spark.openlineage.facets.spark.logicalPlan.disabled", "true");
    props.put("spark.openlineage.facets.spark_unknown.disabled", "true");
    props.put("spark.openlineage.facets.debug.disabled", "true");
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

    GenericContainer<?> container =
        createSparkContainer(
            command,
            c -> {
              c.withFileSystemBind(
                  HOST_TEST_CLASS_OUTPUT_DIR.toString(),
                  CONTAINER_TMP_DIR.toString(),
                  BindMode.READ_WRITE);
              c.withStartupTimeout(Duration.ofSeconds(30));
            });
    container.start();
    container.stop();
    container.close();

    List<String> events = server.events();
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
  private static GenericContainer<?> createSparkContainer(
      List<String> submitCommand, Consumer<GenericContainer<?>> customizer) {
    String command = String.join(" ", submitCommand);
    log.info("Container will be started with command: {}", command);

    GenericContainer<?> container =
        new GenericContainer<>(DockerImageName.parse(SPARK_DOCKER_IMAGE))
            .withNetwork(network)
            .withNetworkAliases("spark")
            .withLogConsumer(SparkContainerUtils::consumeOutput)
            .waitingFor(Wait.forLogMessage(SPARK_DOCKER_CONTAINER_WAIT_MESSAGE, 1))
            .withStartupTimeout(Duration.of(2, ChronoUnit.MINUTES))
            .withCommand(command);

    // mount the additional Jars
    log.info("Mount: {} to {}", HOST_SCALA_FIXTURES_JAR_PATH, CONTAINER_FIXTURES_JAR_PATH);
    container.withFileSystemBind(
        HOST_SCALA_FIXTURES_JAR_PATH.toString(),
        CONTAINER_FIXTURES_JAR_PATH.toString(),
        BindMode.READ_ONLY);

    {
      try (Stream<Path> files = Files.list(HOST_LIB_DIR)) {
        Iterator<Path> iterator = files.iterator();
        while (iterator.hasNext()) {
          Path filePath = iterator.next();
          Path hostPath = filePath.toAbsolutePath();
          Path fileName = hostPath.getFileName();
          Path containerPath = CONTAINER_SPARK_JARS_DIR.resolve(fileName);
          log.info("Mount: {} to {}", hostPath, containerPath);
          container.withFileSystemBind(
              hostPath.toString(), containerPath.toString(), BindMode.READ_ONLY);
        }
      }
    }
    {
      Path additionalJarsDir = Paths.get(System.getProperty("additional.jars.dir"));
      try (Stream<Path> files = Files.list(additionalJarsDir)) {
        Iterator<Path> iterator = files.iterator();
        while (iterator.hasNext()) {
          Path filePath = iterator.next();
          Path hostPath = filePath.toAbsolutePath();
          Path fileName = hostPath.getFileName();
          Path containerPath = CONTAINER_SPARK_JARS_DIR.resolve(fileName);
          log.info("Mount: {} to {}", hostPath, containerPath);
          container.withFileSystemBind(
              hostPath.toString(), containerPath.toString(), BindMode.READ_ONLY);
        }
      }
    }

    customizer.accept(container);

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
