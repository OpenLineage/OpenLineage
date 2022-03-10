package io.openlineage.spark.agent;

import java.io.IOException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.stream.Stream;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.MockServerContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

public class SparkContainerUtils {

  static MockServerContainer makeMockServerContainer(Network network) {
    return new MockServerContainer(
            DockerImageName.parse("jamesdbloom/mockserver:mockserver-5.12.0"))
        .withNetwork(network)
        .withNetworkAliases("openlineageclient");
  }

  private static GenericContainer<?> makePysparkContainer(
      Network network,
      String waitMessage,
      MockServerContainer mockServerContainer,
      String... command) {
    return new GenericContainer<>(
            DockerImageName.parse("godatadriven/pyspark:" + System.getProperty("spark.version")))
        .withNetwork(network)
        .withNetworkAliases("spark")
        .withFileSystemBind("src/test/resources/test_data", "/test_data")
        .withFileSystemBind("src/test/resources/spark_scripts", "/opt/spark_scripts")
        .withFileSystemBind("build/libs", "/opt/libs")
        .withFileSystemBind("build/dependencies", "/opt/dependencies")
        .withLogConsumer(SparkContainerUtils::consumeOutput)
        .waitingFor(Wait.forLogMessage(waitMessage, 1))
        .withStartupTimeout(Duration.of(5, ChronoUnit.MINUTES))
        .dependsOn(mockServerContainer)
        .withReuse(true)
        .withCommand(command);
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
      String... command) {
    return makePysparkContainerWithDefaultConf(
        network,
        "http://openlineageclient:1080",
        ".*ShutdownHookManager: Shutdown hook called.*",
        mockServerContainer,
        namespace,
        command);
  }

  static GenericContainer<?> makePysparkContainerWithDefaultConf(
      Network network,
      String openlineageUrl,
      String waitMessage,
      MockServerContainer mockServerContainer,
      String namespace,
      String... command) {
    return makePysparkContainer(
        network,
        waitMessage,
        mockServerContainer,
        Stream.of(
                new String[] {
                  "--master",
                  "local",
                  "--conf",
                  "spark.openlineage.host=" + openlineageUrl,
                  "--conf",
                  "spark.openlineage.url=" + openlineageUrl + "/api/v1/namespaces/" + namespace,
                  "--conf",
                  "spark.extraListeners=" + OpenLineageSparkListener.class.getName(),
                  "--conf",
                  "spark.sql.warehouse.dir=/tmp/warehouse",
                  "--conf",
                  "spark.sql.shuffle.partitions=1",
                  "--jars",
                  "/opt/libs/"
                      + System.getProperty("openlineage.spark.jar")
                      + ",/opt/dependencies/spark-sql-kafka-*.jar"
                      + ",/opt/dependencies/kafka-*.jar"
                      + ",/opt/dependencies/spark-token-provider-*.jar"
                      + ",/opt/dependencies/commons-pool2-*.jar"
                },
                command)
            .flatMap(Stream::of)
            .toArray(String[]::new));
  }

  static void runPysparkContainerWithDefaultConf(
      Network network,
      MockServerContainer mockServerContainer,
      String namespace,
      String pysparkFile) {
    makePysparkContainerWithDefaultConf(
            network, mockServerContainer, namespace, "/opt/spark_scripts/" + pysparkFile)
        .start();
  }

  private static void consumeOutput(org.testcontainers.containers.output.OutputFrame of) {
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
