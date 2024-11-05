/*
/* Copyright 2018-2024 contributors to the OpenLineage project
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
import java.util.Properties;
import lombok.SneakyThrows;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.MockServerContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.lifecycle.Startable;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

public class FlinkContainerUtils {

  private static final String CONFLUENT_VERSION = "7.6.0";
  private static final String SCHEMA_REGISTRY_IMAGE = getRegistryImage();
  private static final String KAFKA_IMAGE = "confluentinc/cp-kafka:7.6.0";
  private static final String ZOOKEEPER_IMAGE = "confluentinc/cp-zookeeper:" + CONFLUENT_VERSION;
  private static final String CASSANDRA_IMAGE = "cassandra:3.11.16";
  private static final String POSTGRES_IMAGE = "postgres";

  static final String FLINK_IMAGE =
      String.format("flink:%s-java11", System.getProperty("flink.version"));

  static MockServerContainer makeMockServerContainer(Network network) {
    return new MockServerContainer(
            DockerImageName.parse("mockserver/mockserver").withTag("mockserver-5.15.0"))
        .withNetwork(network)
        .withNetworkAliases("openlineageclient");
  }

  static GenericContainer<?> makeSchemaRegistryContainer(Network network, Startable startable) {
    return genericContainer(network, SCHEMA_REGISTRY_IMAGE, "schema-registry")
        .withExposedPorts(28081)
        .withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "PLAINTEXT://kafka-host:9092")
        .withEnv("SCHEMA_REGISTRY_HOST_NAME", "schema-registry")
        .withEnv("SCHEMA_REGISTRY_LISTENERS", "http://schema-registry:8081,http://0.0.0.0:28081")
        .withEnv("SCHEMA_REGISTRY_LOG4J_ROOT_LOGLEVEL", "WARN")
        .dependsOn(startable);
  }

  static GenericContainer<?> makeKafkaContainer(Network network, Startable zookeeper) {
    return genericContainer(network, KAFKA_IMAGE, "kafka-host")
        .withExposedPorts(9092, 19092)
        .withEnv(
            "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP",
            "LISTENER_DOCKER_INTERNAL:PLAINTEXT,LISTENER_DOCKER_EXTERNAL:PLAINTEXT")
        .withEnv(
            "KAFKA_LISTENERS",
            "LISTENER_DOCKER_INTERNAL://kafka-host:9092,LISTENER_DOCKER_EXTERNAL://127.0.0.1:19092")
        .withEnv(
            "KAFKA_ADVERTISED_LISTENERS",
            "LISTENER_DOCKER_INTERNAL://kafka-host:9092,LISTENER_DOCKER_EXTERNAL://127.0.0.1:19092")
        .withEnv("KAFKA_INTER_BROKER_LISTENER_NAME", "LISTENER_DOCKER_INTERNAL")
        .withEnv("KAFKA_ZOOKEEPER_CONNECT", "zookeeper:2181")
        .withEnv("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1")
        .withEnv("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0")
        .withEnv("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
        .withEnv("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
        .withEnv("TOPIC_AUTO_CREATE", "true")
        .withEnv("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1")
        .withEnv("KAFKA_BROKER_ID", "1")
        .withEnv(
            "KAFKA_LOG4J_LOGGERS",
            "kafka.controller=WARN,kafka.producer.async.DefaultEventHandler=WARN,state.change.logger=INFO")
        .withEnv("LOG4J_LOGGER_KAFKA", "WARN")
        .dependsOn(zookeeper);
  }

  @SneakyThrows
  static GenericContainer<?> makeGenerateEventsContainer(Network network, Startable initTopics) {
    return genericContainer(network, SCHEMA_REGISTRY_IMAGE, "generate-events")
        .withCopyFileToContainer(
            MountableFile.forHostPath(Resources.getResource("InputEvent.avsc").getPath()),
            "/tmp/InputEvent.avsc")
        .withCopyFileToContainer(
            MountableFile.forHostPath(Resources.getResource("InputEvent.proto").getPath()),
            "/tmp/InputEvent.proto")
        .withCopyFileToContainer(
            MountableFile.forHostPath(Resources.getResource("events.json").getPath()),
            "/tmp/events.json")
        .withCopyFileToContainer(
            MountableFile.forHostPath(Resources.getResource("events_proto.json").getPath()),
            "/tmp/events_proto.json")
        .withCommand(
            "/bin/bash",
            "-c",
            Resources.toString(Resources.getResource("generate_events.sh"), StandardCharsets.UTF_8))
        .withEnv("SCHEMA_REGISTRY_LOG4J_ROOT_LOGLEVEL", "WARN")
        .dependsOn(initTopics);
  }

  static GenericContainer<?> makeZookeeperContainer(Network network) {
    return genericContainer(network, ZOOKEEPER_IMAGE, "zookeeper")
        .withExposedPorts(2181)
        .withEnv("ZOOKEEPER_CLIENT_PORT", "2181")
        .withEnv("ZOOKEEPER_SERVER_ID", "1")
        .withEnv("ZOOKEEPER_LOG4J_ROOT_LOGLEVEL", "WARN");
  }

  static GenericContainer<?> makeJdbcContainer(Network network) {
    try {
      return genericContainer(network, POSTGRES_IMAGE, "postgres")
          .withExposedPorts(5432)
          .withEnv("POSTGRES_PASSWORD", "postgres");
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  static GenericContainer<?> makeJdbcGenerateRecordContainer(Network network, Startable postgres) {
    try {
      return genericContainer(network, POSTGRES_IMAGE, "postgres_prep")
          .withEnv("PGPASSWORD", "postgres")
          .withCopyFileToContainer(
              MountableFile.forClasspathResource("create_postgres_data.sh"), "/opt/")
          .withCommand("sh", "/opt/create_postgres_data.sh")
          .dependsOn(postgres);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  static GenericContainer<?> makeCassandraContainer(Network network) {
    try {
      GenericContainer container =
          genericContainer(network, CASSANDRA_IMAGE, "cassandra-server")
              .withExposedPorts(9042)
              .withEnv("CASSANDRA_SNITCH", "GossipingPropertyFileSnitch")
              .withEnv(
                  "JVM_OPTS",
                  "-Dcassandra.skip_wait_for_gossip_to_settle=0 -Dcassandra.initial_token=0")
              .withEnv("HEAP_NEWSIZE", "128M")
              .withEnv("MAX_HEAP_SIZE", "1024M")
              .withEnv("CASSANDRA_ENDPOINT_SNITCH", "GossipingPropertyFileSnitch")
              .withEnv("CASSANDRA_DC", "datacenter1");

      return container;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  static GenericContainer<?> makeCassandraGenerateRecordContainer(
      Network network, Startable cassandra) {
    return genericContainer(network, CASSANDRA_IMAGE, "cassandra_prepare")
        .withCopyFileToContainer(
            MountableFile.forClasspathResource("create_cassandra_data.sh"), "/opt/cassandra/")
        .withCommand("sh", "/opt/cassandra/create_cassandra_data.sh")
        .dependsOn(cassandra);
  }

  static GenericContainer<?> makeFlinkJobManagerContainer(
      String entrypointClass,
      Network network,
      List<Startable> startables,
      Properties jobProperties) {

    String inputTopics =
        jobProperties.getProperty(
            "inputTopics", "io.openlineage.flink.kafka.input1,io.openlineage.flink.kafka.input2");
    String outputTopics =
        jobProperties.getProperty("outputTopics", "io.openlineage.flink.kafka.output");
    String jobNameParam = "";
    if (jobProperties.getProperty("jobName") != null) {
      jobNameParam = "--job-name " + jobProperties.get("jobName") + " ";
    }
    String configPath = jobProperties.getProperty("configPath", "/opt/flink/lib/openlineage.yml");
    GenericContainer<?> container =
        genericContainer(network, FLINK_IMAGE, "jobmanager")
            .withExposedPorts(8081)
            .withFileSystemBind(getOpenLineageJarPath(), "/opt/flink/lib/openlineage.jar")
            .withFileSystemBind(getExampleAppJarPath(), "/opt/flink/lib/example-app.jar")
            .withCopyFileToContainer(
                MountableFile.forHostPath("../data/iceberg"), "/tmp/warehouse/")
            .withCopyFileToContainer(
                MountableFile.forHostPath(Resources.getResource("openlineage.yml").getPath()),
                configPath)
            .withCopyFileToContainer(
                MountableFile.forHostPath(
                    Resources.getResource("log4j-console.properties").getPath()),
                "/opt/flink/conf/log4j-console.properties")
            .withCommand(
                "standalone-job "
                    + String.format("--job-classname %s ", entrypointClass)
                    + "--input-topics "
                    + inputTopics
                    + " --output-topics "
                    + outputTopics
                    + " "
                    + jobNameParam)
            .withEnv(
                "FLINK_PROPERTIES", "jobmanager.rpc.address: jobmanager\nexecution.attached: true")
            .withEnv("OPENLINEAGE_CONFIG", configPath)
            .withStartupTimeout(Duration.of(5, ChronoUnit.MINUTES))
            .dependsOn(startables);
    return container;
  }

  static GenericContainer<?> makeFlinkTaskManagerContainer(
      Network network, List<Startable> startables) {
    return genericContainer(network, FLINK_IMAGE, "taskmanager")
        .withFileSystemBind(getOpenLineageJarPath(), "/opt/flink/lib/openlineage.jar")
        .withFileSystemBind(getExampleAppJarPath(), "/opt/flink/lib/example-app.jar")
        .withCopyFileToContainer(MountableFile.forHostPath("../data/iceberg"), "/tmp/warehouse/")
        .withCopyFileToContainer(
            MountableFile.forHostPath(Resources.getResource("log4j-console.properties").getPath()),
            "/opt/flink/conf/log4j-console.properties")
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

  static boolean verifyJobManagerReachedCheckpointOrFinished(GenericContainer jobManager) {
    String logs = jobManager.getLogs();

    // list of log entries that should stop the test
    return logs.contains("New checkpoint encountered")
        || logs.contains("Shutting down remote daemon.")
        || logs.contains(
            "Terminating cluster entrypoint process StandaloneApplicationClusterEntryPoint with exit code 0.");
  }

  static GenericContainer<?> genericContainer(Network network, String image, String hostname) {
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
          System.out.println(of.getUtf8String()); // NOPMD
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

  static String getOpenLineageJarPath() {
    return Arrays.stream((new File("build/libs")).listFiles())
        .filter(file -> file.getName().startsWith("openlineage-flink"))
        .map(file -> file.getPath())
        .findAny()
        .get();
  }

  static String getExampleAppJarPath() {
    return Arrays.stream((new File("../fixtures")).listFiles())
        .filter(file -> file.getName().startsWith("flink-examples-stateful"))
        .map(file -> file.getPath())
        .findAny()
        .get();
  }

  private static String getRegistryImage() {
    return "confluentinc/cp-schema-registry:" + CONFLUENT_VERSION;
  }
}
