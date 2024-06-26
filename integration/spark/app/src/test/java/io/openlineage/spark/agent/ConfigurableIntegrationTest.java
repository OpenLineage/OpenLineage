/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent;

import static io.openlineage.spark.agent.SparkContainerProperties.CONTAINER_LOG4J2_PROPERTIES_PATH;
import static io.openlineage.spark.agent.SparkContainerProperties.CONTAINER_LOG4J_PROPERTIES_PATH;
import static io.openlineage.spark.agent.SparkContainerProperties.HOST_LOG4J2_PROPERTIES_PATH;
import static io.openlineage.spark.agent.SparkContainerProperties.HOST_LOG4J_PROPERTIES_PATH;
import static io.openlineage.spark.agent.SparkContainerProperties.SPARK_DOCKER_IMAGE;
import static io.openlineage.spark.agent.SparkContainerUtils.SPARK_DOCKER_CONTAINER_WAIT_MESSAGE;
import static io.openlineage.spark.agent.SparkContainerUtils.mountPath;
import static org.assertj.core.api.Assertions.assertThat;

import io.openlineage.spark.agent.util.ConfigurableTestConfig;
import io.openlineage.spark.agent.util.RunEventVerifier;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Stream;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.OutputFrame;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

/**
 * Contains generic & configurable integration test which can be triggered through CLI script to
 * assert if given Spark SQL script generates desired OpenLineage events.
 */
@Tag("integration-test")
@Tag("configurable-integration-test")
@Slf4j
class ConfigurableIntegrationTest {

  public static final String OPENLINEAGE_ROOT_DIR = "/usr/lib/openlineage";
  /**
   * Name of the tests within the examples which is expected fail. Running it in CI makes assures a
   * testing mechanism is working properly.
   */
  private static final String FAILING_TEST = "test_failing";

  private ConfigurableTestConfig config;

  @SneakyThrows
  @ParameterizedTest
  @MethodSource("provideTestDirs")
  void run(Path testDir) {
    config = ConfigurableTestConfig.load(testDir.toString(), System.getProperty("spark.conf.file"));
    Path runDir = config.generateRunDir(testDir);
    OutputStreamWriter fileLogger = new FileWriter(runDir + "/output.log", true);
    fileLogger.write("Parsed configuration: " + config);

    makePysparkContainer(runDir, fileLogger).start();
    RunEventVerifier eventVerifier = RunEventVerifier.of(new File(runDir + "/events.json"));
    boolean match = eventVerifier.match(config.getExpectationJsons().toArray(new File[] {}));

    if (!match) {
      // append diff to test output files
      fileLogger.write(eventVerifier.getDiffDescription());
    }

    fileLogger.flush();

    if (testDir.endsWith(FAILING_TEST)) {
      // assure the test is failing
      assertThat(match).isFalse();
    } else {
      assertThat(match).isTrue();
    }
  }

  @SneakyThrows
  private static Stream<Arguments> provideTestDirs() {
    return Files.list(Paths.get(System.getProperty("test.dir")))
        .filter(path -> new File(path.toString()).isDirectory())
        .filter(
            path -> {
              try {
                boolean hasJsonFile =
                    Files.list(path)
                        .filter(p -> p.toString().toLowerCase(Locale.ROOT).endsWith(".json"))
                        .findAny()
                        .isPresent();

                if (!hasJsonFile) {
                  log.warn(
                      "Test directory {} does not contain any JSON file to verify events emitted",
                      path);
                }

                boolean hasJobFile =
                    Files.list(path)
                        .map(p -> p.toString().toLowerCase(Locale.ROOT))
                        .filter(f -> f.endsWith(".sql") || f.endsWith(".py"))
                        .findAny()
                        .isPresent();

                if (!hasJobFile) {
                  log.warn("Test directory {} does not contain any job file (.sql or .py)", path);
                }

                return hasJobFile && hasJsonFile;
              } catch (IOException e) {
                log.error("Couldn't list directory {}", path);
              }
              return false;
            })
        .map(Arguments::of);
  }

  @SneakyThrows
  private GenericContainer<?> makePysparkContainer(Path runDir, Writer output) {
    List<String> command =
        new ArrayList<>(
            Arrays.asList(
                "./bin/spark-submit",
                "--master",
                "local",
                "--conf",
                "spark.openlineage.transport.type=file",
                "--conf",
                "spark.openlineage.transport.location=" + runDir.toString() + "/events.json",
                "--conf",
                "spark.jars.ivy=" + OPENLINEAGE_ROOT_DIR + "/integration/spark/cli/.ivy2",
                "--conf",
                "spark.extraListeners=" + OpenLineageSparkListener.class.getName()));

    Optional<Path> olJar =
        Files.list(Paths.get(OPENLINEAGE_ROOT_DIR + "/integration/spark/app/build/libs/shadow"))
            .filter(f -> f.getFileName().toString().endsWith(".jar"))
            .findFirst();

    if (config.getPackages() != null) {
      command.add("--packages");
      command.add(String.join(",", config.getPackages()));
    }

    command.add("--jars");
    command.add(olJar.get().toAbsolutePath().toString());
    command.add(getPySparkFile(runDir).getPath());

    output.write("command: " + String.join(" ", command));
    GenericContainer container =
        new GenericContainer<>(DockerImageName.parse(SPARK_DOCKER_IMAGE))
            .withLogConsumer(
                new Consumer<OutputFrame>() {
                  @Override
                  @SneakyThrows
                  public void accept(OutputFrame outputFrame) {
                    output.write(outputFrame.getUtf8String());
                  }
                })
            .waitingFor(Wait.forLogMessage(SPARK_DOCKER_CONTAINER_WAIT_MESSAGE, 1))
            .withStartupTimeout(Duration.of(10, ChronoUnit.MINUTES))
            .withCommand(command.toArray(new String[] {}));

    log.debug("Host root openlineage dir is {}", System.getProperty("host.dir"));
    container.withFileSystemBind(
        System.getProperty("host.dir"), OPENLINEAGE_ROOT_DIR, BindMode.READ_WRITE);

    mountPath(
        container,
        convertToHostDirPath(HOST_LOG4J_PROPERTIES_PATH),
        CONTAINER_LOG4J_PROPERTIES_PATH);
    mountPath(
        container,
        convertToHostDirPath(HOST_LOG4J2_PROPERTIES_PATH),
        CONTAINER_LOG4J2_PROPERTIES_PATH);

    return container;
  }

  private Path convertToHostDirPath(Path path) {
    return Paths.get(path.toString().replace(OPENLINEAGE_ROOT_DIR, System.getProperty("host.dir")));
  }

  @SneakyThrows
  private File getPySparkFile(Path runDir) {
    String scriptContent = config.generatePySparkCode();

    String scriptName = config.getInputScript().getName().replace(".sql", ".py");
    File pySpark = new File(runDir.toString() + "/" + scriptName);

    Files.write(pySpark.toPath(), scriptContent.getBytes(StandardCharsets.UTF_8));
    return pySpark;
  }
}
