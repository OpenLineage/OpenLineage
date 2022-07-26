/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink;

import com.google.common.io.Resources;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.List;
import lombok.SneakyThrows;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.MockServerContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.lifecycle.Startable;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

public class FlinkContainerUtils {

  private static final String CONFLUENT_VERSION = "6.2.1";
  private static final String SCHEMA_REGISTRY_IMAGE = getRegistryImage();
  private static final String KAFKA_IMAGE = "wurstmeister/kafka:2.13-2.8.1";
  private static final String ZOOKEEPER_IMAGE = "confluentinc/cp-zookeeper:" + CONFLUENT_VERSION;
  private static final String FLINK_IMAGE = "flink:1.14.4-scala_2.12-java11";

  static MockServerContainer makeMockServerContainer(Network network) {
    return new MockServerContainer(
            DockerImageName.parse("jamesdbloom/mockserver:mockserver-5.12.0"))
        .withLogConsumer(of -> consumeOutput("mockserver", of))
        .withNetwork(network)
        .withNetworkAliases("openlineageclient");
  }

  static GenericContainer<?> makeSchemaRegistryContainer(Network network, Startable startable) {
    return genericContainer(network, SCHEMA_REGISTRY_IMAGE, "schema-registry")
        .withExposedPorts(28081)
        .withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "PLAINTEXT://kafka:9092")
        .withEnv("SCHEMA_REGISTRY_HOST_NAME", "schema-registry")
        .withEnv("SCHEMA_REGISTRY_LISTENERS", "http://schema-registry:8081,http://0.0.0.0:28081")
        .dependsOn(startable);
  }

  static GenericContainer<?> makeKafkaContainer(Network network, Startable zookeeper) {
    return genericContainer(network, KAFKA_IMAGE, "kafka")
        .withExposedPorts(9092, 19092)
        .withEnv(
            "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP",
            "LISTENER_DOCKER_INTERNAL:PLAINTEXT,LISTENER_DOCKER_EXTERNAL:PLAINTEXT")
        .withEnv(
            "KAFKA_LISTENERS",
            "LISTENER_DOCKER_INTERNAL://kafka:9092,LISTENER_DOCKER_EXTERNAL://127.0.0.1:19092")
        .withEnv(
            "KAFKA_ADVERTISED_LISTENERS",
            "LISTENER_DOCKER_INTERNAL://kafka:9092,LISTENER_DOCKER_EXTERNAL://127.0.0.1:19092")
        .withEnv("KAFKA_INTER_BROKER_LISTENER_NAME", "LISTENER_DOCKER_INTERNAL")
        .withEnv("KAFKA_ZOOKEEPER_CONNECT", "zookeeper:2181")
        .withEnv("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1")
        .withEnv("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0")
        .withEnv("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
        .withEnv("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
        .withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "true")
        .withEnv("KAFKA_BROKER_ID", "1")
        .withEnv(
            "KAFKA_CREATE_TOPICS",
            "io.openlineage.flink.kafka.input1:1:1,io.openlineage.flink.kafka.input2:1:1,io.openlineage.flink.kafka.output:1:1")
        .withEnv(
            "KAFKA_LOG4J_LOGGERS",
            "kafka.controller=INFO,kafka.producer.async.DefaultEventHandler=INFO,state.change.logger=INFO")
        .dependsOn(zookeeper);
  }

  @SneakyThrows
  static GenericContainer<?> makeGenerateEventsContainer(Network network, Startable initTopics) {
    return genericContainer(network, SCHEMA_REGISTRY_IMAGE, "generate-events")
        .withCopyFileToContainer(
            MountableFile.forHostPath(Resources.getResource("InputEvent.avsc").getPath()),
            "/tmp/InputEvent.avsc")
        .withCopyFileToContainer(
            MountableFile.forHostPath(Resources.getResource("events.json").getPath()),
            "/tmp/events.json")
        .withCommand(
            "/bin/sh",
            "-c",
            Resources.toString(Resources.getResource("generate_events.sh"), StandardCharsets.UTF_8))
        .dependsOn(initTopics);
  }

  static GenericContainer<?> makeZookeeperContainer(Network network) {
    return genericContainer(network, ZOOKEEPER_IMAGE, "zookeeper")
        .withExposedPorts(2181)
        .withEnv("ZOOKEEPER_CLIENT_PORT", "2181")
        .withEnv("ZOOKEEPER_SERVER_ID", "1");
  }

  static GenericContainer<?> makeFlinkJobManagerContainer(
      String jobName, String configPath, Network network, List<Startable> startables) {
    GenericContainer<?> container =
        genericContainer(network, FLINK_IMAGE, "jobmanager")
            .withExposedPorts(8081)
            .withFileSystemBind(getOpenLineageJarPath(), "/opt/flink/lib/openlineage.jar")
            .withFileSystemBind(getExampleAppJarPath(), "/opt/flink/lib/example-app.jar")
            .withFileSystemBind("/tmp/warehouse", "/tmp/warehouse/", BindMode.READ_WRITE)
            .withCopyFileToContainer(
                MountableFile.forHostPath(Resources.getResource("openlineage.yml").getPath()),
                configPath)
            .withCommand(
                "standalone-job "
                    + String.format("--job-classname %s ", jobName)
                    + "--input-topics io.openlineage.flink.kafka.input1,io.openlineage.flink.kafka.input2 "
                    + "--output-topic io.openlineage.flink.kafka.output ")
            .withEnv(
                "FLINK_PROPERTIES", "jobmanager.rpc.address: jobmanager\nexecution.attached: true")
            .withEnv("OPENLINEAGE_CONFIG", configPath)
            .withStartupTimeout(Duration.of(5, ChronoUnit.MINUTES))
            .dependsOn(startables);
    return container;
  }

  static GenericContainer<?> makeFlinkJobManagerContainer(
      String jobName, Network network, List<Startable> startables) {
    return makeFlinkJobManagerContainer(
        jobName, "/opt/flink/lib/openlineage.yml", network, startables);
  }

  static GenericContainer<?> makeFlinkTaskManagerContainer(
      Network network, List<Startable> startables) {
    return genericContainer(network, FLINK_IMAGE, "taskmanager")
        .withFileSystemBind(getOpenLineageJarPath(), "/opt/flink/lib/openlineage.jar")
        .withFileSystemBind(getExampleAppJarPath(), "/opt/flink/lib/example-app.jar")
        .withFileSystemBind("/tmp/warehouse", "/tmp/warehouse/", BindMode.READ_WRITE)
        .withEnv(
            "FLINK_PROPERTIES",
            "jobmanager.rpc.address: jobmanager"
                + System.lineSeparator()
                + "taskmanager.numberOfTaskSlots: 2")
        .withCommand("taskmanager")
        .dependsOn(startables);
  }

  static void stopAll(List<GenericContainer<?>> containers) {
    containers.stream()
        .forEach(
            container -> {
              try {
                container.stop();
              } catch (Exception e) {
                // do nothing, perhaps already stopped
              }
            });
  }

  private static GenericContainer<?> genericContainer(
      Network network, String image, String hostname) {
    return new GenericContainer<>(DockerImageName.parse(image))
        .withNetwork(network)
        .withLogConsumer(of -> consumeOutput(hostname, of))
        .withNetworkAliases(hostname)
        .withReuse(true);
  }

  private static void consumeOutput(
      String prefix, org.testcontainers.containers.output.OutputFrame of) {
    try {
      switch (of.getType()) {
        case STDOUT:
          System.out.write(prefixEachLine(prefix, of.getUtf8String()).getBytes());
          break;
        case STDERR:
          System.err.write(prefixEachLine(prefix, of.getUtf8String()).getBytes());
          break;
        case END:
          System.out.println(of.getUtf8String());
          break;
      }
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }

  private static String prefixEachLine(String prefix, String output) {
    String prefixTag = "[" + prefix + "]";
    return prefixTag + output.replace(System.lineSeparator(), System.lineSeparator() + prefixTag);
  }

  private static String getOpenLineageJarPath() {
    return Arrays.stream((new File("build/libs")).listFiles())
        .filter(file -> file.getName().startsWith("openlineage-flink"))
        .map(file -> file.getPath())
        .findAny()
        .get();
  }

  private static String getExampleAppJarPath() {
    return Arrays.stream((new File("examples/stateful/build/libs")).listFiles())
        .filter(file -> file.getName().startsWith("stateful"))
        .map(file -> file.getPath())
        .findAny()
        .get();
  }

  private static String getRegistryImage() {
    if (System.getProperty("os.arch").equals("aarch64")) {
      return "eugenetea/schema-registry-arm64:latest";
    }
    return "confluentinc/cp-schema-registry:" + CONFLUENT_VERSION;
  }
}
