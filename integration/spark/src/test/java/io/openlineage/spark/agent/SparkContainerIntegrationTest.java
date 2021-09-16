package io.openlineage.spark.agent;

import static java.nio.file.Files.readAllBytes;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.JsonBody.json;

import io.openlineage.spark.agent.client.OpenLineageClient;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockserver.client.MockServerClient;
import org.mockserver.matchers.MatchType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.MockServerContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Tag("integration-test")
@Testcontainers
public class SparkContainerIntegrationTest {

  private static final Network network = Network.newNetwork();

  @Container
  private static final MockServerContainer openLineageClientMockContainer =
      makeMockServerContainer();

  private static GenericContainer<?> pyspark;
  private static GenericContainer<?> kafka;
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
  }

  @AfterEach
  public void cleanupSpark() {
    pyspark.stop();
  }

  @AfterAll
  public static void tearDown() {
    Logger logger = LoggerFactory.getLogger(SparkContainerIntegrationTest.class);
    try {
      openLineageClientMockContainer.stop();
    } catch (Exception e2) {
      logger.error("Unable to shut down openlineage client container", e2);
    }
    try {
      pyspark.stop();
    } catch (Exception e2) {
      logger.error("Unable to shut down pyspark container", e2);
    }
    try {
      kafka.stop();
    } catch (Exception e2) {
      logger.error("Unable to shut down kafka container", e2);
    }
    network.close();
  }

  private static MockServerContainer makeMockServerContainer() {
    return new MockServerContainer(
            DockerImageName.parse("jamesdbloom/mockserver:mockserver-5.11.2"))
        .withNetwork(network)
        .withNetworkAliases("openlineageclient");
  }

  private static GenericContainer<?> makePysparkContainer(String... command) {
    return new GenericContainer<>(
            DockerImageName.parse("godatadriven/pyspark:" + System.getProperty("spark.version")))
        .withNetwork(network)
        .withNetworkAliases("spark")
        .withFileSystemBind("src/test/resources/test_data", "/test_data")
        .withFileSystemBind("src/test/resources/spark_scripts", "/opt/spark_scripts")
        .withFileSystemBind("build/libs", "/opt/libs")
        .withLogConsumer(SparkContainerIntegrationTest::consumeOutput)
        .withStartupTimeout(Duration.of(2, ChronoUnit.MINUTES))
        .dependsOn(openLineageClientMockContainer)
        .withReuse(true)
        .withCommand(command);
  }

  private static GenericContainer<?> makeKafkaContainer() {
    return new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:5.4.3"))
        .withNetworkAliases("kafka")
        .withNetwork(network);
  }

  private static GenericContainer<?> makePysparkContainerWithDefaultConf(
      String namespace, String... command) {
    ArrayList<String> defaults =
        new ArrayList<>(
            Arrays.asList(
                "--master",
                "local",
                "--conf",
                "spark.openlineage.host=" + "http://openlineageclient:1080",
                "--conf",
                "spark.openlineage.url="
                    + "http://openlineageclient:1080/api/v1/namespaces/"
                    + namespace,
                "--conf",
                "spark.extraListeners=" + OpenLineageSparkListener.class.getName(),
                "--conf",
                "spark.sql.warehouse.dir=/tmp/warehouse",
                "--jars",
                "/opt/libs/" + System.getProperty("openlineage.spark.jar")));
    defaults.addAll(Arrays.asList(command));
    return makePysparkContainer(defaults.toArray(new String[0]));
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

  @Test
  public void testPysparkWordCountWithCliArgs() throws IOException, InterruptedException {
    pyspark =
        makePysparkContainerWithDefaultConf(
            "testPysparkWordCountWithCliArgs", "/opt/spark_scripts/spark_word_count.py");
    pyspark.setWaitStrategy(Wait.forLogMessage(".*ShutdownHookManager: Shutdown hook called.*", 1));
    pyspark.start();

    Path eventFolder = Paths.get("integrations/container/");
    String startEvent =
        new String(readAllBytes(eventFolder.resolve("pysparkWordCountWithCliArgsStartEvent.json")))
            .replaceAll(
                "https://github.com/OpenLineage/OpenLineage/tree/\\$VERSION/integration/spark",
                OpenLineageClient.OPEN_LINEAGE_CLIENT_URI.toString());
    String completeEvent =
        new String(
                readAllBytes(eventFolder.resolve("pysparkWordCountWithCliArgsCompleteEvent.json")))
            .replaceAll(
                "https://github.com/OpenLineage/OpenLineage/tree/\\$VERSION/integration/spark",
                OpenLineageClient.OPEN_LINEAGE_CLIENT_URI.toString());
    mockServerClient.verify(
        request()
            .withPath("/api/v1/lineage")
            .withBody(json(startEvent, MatchType.ONLY_MATCHING_FIELDS)),
        request()
            .withPath("/api/v1/lineage")
            .withBody(json(completeEvent, MatchType.ONLY_MATCHING_FIELDS)));
  }

  @Test
  public void testPysparkRddToTable() throws IOException, InterruptedException {
    pyspark =
        makePysparkContainerWithDefaultConf(
            "testPysparkRddToTable", "/opt/spark_scripts/spark_rdd_to_table.py");
    pyspark.setWaitStrategy(Wait.forLogMessage(".*ShutdownHookManager: Shutdown hook called.*", 1));
    pyspark.start();

    Path eventFolder = Paths.get("integrations/container/");
    String startCsvEvent =
        new String(readAllBytes(eventFolder.resolve("pysparkRddToCsvStartEvent.json")))
            .replaceAll(
                "https://github.com/OpenLineage/OpenLineage/tree/\\$VERSION/integration/spark",
                OpenLineageClient.OPEN_LINEAGE_CLIENT_URI.toString());
    String completeCsvEvent =
        new String(readAllBytes(eventFolder.resolve("pysparkRddToCsvCompleteEvent.json")))
            .replaceAll(
                "https://github.com/OpenLineage/OpenLineage/tree/\\$VERSION/integration/spark",
                OpenLineageClient.OPEN_LINEAGE_CLIENT_URI.toString());

    String startTableEvent =
        new String(readAllBytes(eventFolder.resolve("pysparkRddToTableStartEvent.json")))
            .replaceAll(
                "https://github.com/OpenLineage/OpenLineage/tree/\\$VERSION/integration/spark",
                OpenLineageClient.OPEN_LINEAGE_CLIENT_URI.toString());

    String completeTableEvent =
        new String(readAllBytes(eventFolder.resolve("pysparkRddToTableCompleteEvent.json")))
            .replaceAll(
                "https://github.com/OpenLineage/OpenLineage/tree/\\$VERSION/integration/spark",
                OpenLineageClient.OPEN_LINEAGE_CLIENT_URI.toString());

    mockServerClient.verify(
        request()
            .withPath("/api/v1/lineage")
            .withBody(json(startCsvEvent, MatchType.ONLY_MATCHING_FIELDS)),
        request()
            .withPath("/api/v1/lineage")
            .withBody(json(completeCsvEvent, MatchType.ONLY_MATCHING_FIELDS)),
        request()
            .withPath("/api/v1/lineage")
            .withBody(json(startTableEvent, MatchType.ONLY_MATCHING_FIELDS)),
        request()
            .withPath("/api/v1/lineage")
            .withBody(json(completeTableEvent, MatchType.ONLY_MATCHING_FIELDS)));
  }

  @Test
  public void testPysparkKafkaReadWrite() throws IOException {
    kafka = makeKafkaContainer();
    kafka.start();

    pyspark =
        makePysparkContainerWithDefaultConf(
            "testPysparkKafkaReadWriteTest",
            "--packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.11:" + System.getProperty("spark.version"),
            "/opt/spark_scripts/spark_kafka.py");

    pyspark.setWaitStrategy(Wait.forLogMessage(".*ShutdownHookManager: Shutdown hook called.*", 1));
    pyspark.start();
  }

  @Test
  public void testPysparkSQLHiveTest() throws IOException, InterruptedException {
    pyspark =
        makePysparkContainerWithDefaultConf(
            "testPysparkSQLHiveTest", "/opt/spark_scripts/spark_hive.py");
    pyspark.setWaitStrategy(Wait.forLogMessage(".*ShutdownHookManager: Shutdown hook called.*", 1));
    pyspark.start();

    Path eventFolder = Paths.get("integrations/container/");

    String startEvent =
        new String(readAllBytes(eventFolder.resolve("pysparkHiveStartEvent.json")))
            .replaceAll(
                "https://github.com/OpenLineage/OpenLineage/tree/\\$VERSION/integration/spark",
                OpenLineageClient.OPEN_LINEAGE_CLIENT_URI.toString());
    String completeEvent =
        new String(readAllBytes(eventFolder.resolve("pysparkHiveCompleteEvent.json")))
            .replaceAll(
                "https://github.com/OpenLineage/OpenLineage/tree/\\$VERSION/integration/spark",
                OpenLineageClient.OPEN_LINEAGE_CLIENT_URI.toString());
    mockServerClient.verify(
        request()
            .withPath("/api/v1/lineage")
            .withBody(json(startEvent, MatchType.ONLY_MATCHING_FIELDS)),
        request()
            .withPath("/api/v1/lineage")
            .withBody(json(completeEvent, MatchType.ONLY_MATCHING_FIELDS)));
  }

  @Test
  public void testPysparkSQLOverwriteDirHiveTest() throws IOException, InterruptedException {
    pyspark =
        makePysparkContainerWithDefaultConf(
            "testPysparkSQLHiveOverwriteDirTest", "/opt/spark_scripts/spark_overwrite_hive.py");
    pyspark.setWaitStrategy(Wait.forLogMessage(".*ShutdownHookManager: Shutdown hook called.*", 1));
    pyspark.start();

    Path eventFolder = Paths.get("integrations/container/");

    String startEvent =
        new String(readAllBytes(eventFolder.resolve("pysparkHiveOverwriteDirStartEvent.json")))
            .replaceAll(
                "https://github.com/OpenLineage/OpenLineage/tree/\\$VERSION/integration/spark",
                OpenLineageClient.OPEN_LINEAGE_CLIENT_URI.toString());
    String completeEvent =
        new String(readAllBytes(eventFolder.resolve("pysparkHiveOverwriteDirCompleteEvent.json")))
            .replaceAll(
                "https://github.com/OpenLineage/OpenLineage/tree/\\$VERSION/integration/spark",
                OpenLineageClient.OPEN_LINEAGE_CLIENT_URI.toString());
    mockServerClient.verify(
        request()
            .withPath("/api/v1/lineage")
            .withBody(json(startEvent, MatchType.ONLY_MATCHING_FIELDS)),
        request()
            .withPath("/api/v1/lineage")
            .withBody(json(completeEvent, MatchType.ONLY_MATCHING_FIELDS)));
  }
}
