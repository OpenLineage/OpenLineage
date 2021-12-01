package io.openlineage.spark.agent;

import static java.nio.file.Files.readAllBytes;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.JsonBody.json;

import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.stream.Stream;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
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
  private static final Logger logger = LoggerFactory.getLogger(SparkContainerIntegrationTest.class);

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
    mockServerClient.reset();
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
  }

  @AfterAll
  public static void tearDown() {
    try {
      openLineageClientMockContainer.stop();
    } catch (Exception e2) {
      logger.error("Unable to shut down openlineage client container", e2);
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
        .withFileSystemBind("build/dependencies", "/opt/dependencies")
        .withLogConsumer(SparkContainerIntegrationTest::consumeOutput)
        .waitingFor(Wait.forLogMessage(".*ShutdownHookManager: Shutdown hook called.*", 1))
        .withStartupTimeout(Duration.of(5, ChronoUnit.MINUTES))
        .dependsOn(openLineageClientMockContainer)
        .withReuse(true)
        .withCommand(command);
  }

  private static GenericContainer<?> makeKafkaContainer() {
    return new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.0.0"))
        .withNetworkAliases("kafka")
        .withNetwork(network);
  }

  private static GenericContainer<?> makePysparkContainerWithDefaultConf(
      String namespace, String... command) {
    return makePysparkContainer(
        Stream.of(
                new String[] {
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
                  "/opt/libs/"
                      + System.getProperty("openlineage.spark.jar")
                      + ",/opt/dependencies/spark-sql-kafka-*.jar"
                      + ",/opt/dependencies/kafka-*.jar"
                      + ",/opt/dependencies/spark-token-provider-*.jar"
                      + ",/opt/dependencies/commons-pool2-*.jar"
                      + ",/opt/dependencies/sqlite-jdbc-*.jar"
                },
                command)
            .flatMap(Stream::of)
            .toArray(String[]::new));
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
    pyspark.start();

    Path eventFolder = Paths.get("integrations/container/");
    String startEvent =
        new String(readAllBytes(eventFolder.resolve("pysparkWordCountWithCliArgsStartEvent.json")));
    String completeEvent =
        new String(
            readAllBytes(eventFolder.resolve("pysparkWordCountWithCliArgsCompleteEvent.json")));
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
    pyspark.start();

    Path eventFolder = Paths.get("integrations/container/");
    String startCsvEvent =
        new String(readAllBytes(eventFolder.resolve("pysparkRddToCsvStartEvent.json")));
    String completeCsvEvent =
        new String(readAllBytes(eventFolder.resolve("pysparkRddToCsvCompleteEvent.json")));

    String startTableEvent =
        new String(readAllBytes(eventFolder.resolve("pysparkRddToTableStartEvent.json")));
    String completeTableEvent =
        new String(readAllBytes(eventFolder.resolve("pysparkRddToTableCompleteEvent.json")));

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
            System.getProperty("kafka.package.version"),
            "/opt/spark_scripts/spark_kafka.py");
    pyspark.start();

    Path eventFolder = Paths.get("integrations/container/");
    String writeStartEvent =
        new String(readAllBytes(eventFolder.resolve("pysparkKafkaWriteStartEvent.json")));
    String writeCompleteEvent =
        new String(readAllBytes(eventFolder.resolve("pysparkKafkaWriteCompleteEvent.json")));
    String readStartEvent =
        new String(readAllBytes(eventFolder.resolve("pysparkKafkaReadStartEvent.json")));
    String readCompleteEvent =
        new String(readAllBytes(eventFolder.resolve("pysparkKafkaReadCompleteEvent.json")));

    mockServerClient.verify(
        request()
            .withPath("/api/v1/lineage")
            .withBody(json(writeStartEvent, MatchType.ONLY_MATCHING_FIELDS)),
        request()
            .withPath("/api/v1/lineage")
            .withBody(json(writeCompleteEvent, MatchType.ONLY_MATCHING_FIELDS)),
        request()
            .withPath("/api/v1/lineage")
            .withBody(json(readStartEvent, MatchType.ONLY_MATCHING_FIELDS)),
        request()
            .withPath("/api/v1/lineage")
            .withBody(json(readCompleteEvent, MatchType.ONLY_MATCHING_FIELDS)));
  }

  @Test
  public void testPysparkKafkaReadAssign() throws IOException {
    kafka = makeKafkaContainer();
    kafka.start();

    ImmutableMap<String, Object> kafkaProps =
        ImmutableMap.of(
            "bootstrap.servers",
            kafka.getHost() + ":" + kafka.getMappedPort(KafkaContainer.KAFKA_PORT));
    AdminClient admin = AdminClient.create(kafkaProps);
    admin.createTopics(
        Arrays.asList(new NewTopic("topicA", 1, (short) 0), new NewTopic("topicB", 1, (short) 0)));
    pyspark =
        makePysparkContainerWithDefaultConf(
            "testPysparkKafkaReadAssignTest", "/opt/spark_scripts/spark_kafk_assign_read.py");
    pyspark.start();

    Path eventFolder = Paths.get("integrations/container/");
    String readStartEvent =
        new String(readAllBytes(eventFolder.resolve("pysparkKafkaAssignReadStartEvent.json")));
    String readCompleteEvent =
        new String(readAllBytes(eventFolder.resolve("pysparkKafkaAssignReadCompleteEvent.json")));

    mockServerClient.verify(
        request()
            .withPath("/api/v1/lineage")
            .withBody(json(readStartEvent, MatchType.ONLY_MATCHING_FIELDS)),
        request()
            .withPath("/api/v1/lineage")
            .withBody(json(readCompleteEvent, MatchType.ONLY_MATCHING_FIELDS)));
  }

  @Test
  public void testPysparkSQLHiveTest() throws IOException, InterruptedException {
    pyspark =
        makePysparkContainerWithDefaultConf(
            "testPysparkSQLHiveTest", "/opt/spark_scripts/spark_hive.py");
    pyspark.start();

    Path eventFolder = Paths.get("integrations/container/");
    String startEvent = new String(readAllBytes(eventFolder.resolve("pysparkHiveStartEvent.json")));
    String completeEvent =
        new String(readAllBytes(eventFolder.resolve("pysparkHiveCompleteEvent.json")));

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
    pyspark.start();

    Path eventFolder = Paths.get("integrations/container/");

    String startEvent =
        new String(readAllBytes(eventFolder.resolve("pysparkHiveOverwriteDirStartEvent.json")));
    String completeEvent =
        new String(readAllBytes(eventFolder.resolve("pysparkHiveOverwriteDirCompleteEvent.json")));
    mockServerClient.verify(
        request()
            .withPath("/api/v1/lineage")
            .withBody(json(startEvent, MatchType.ONLY_MATCHING_FIELDS)),
        request()
            .withPath("/api/v1/lineage")
            .withBody(json(completeEvent, MatchType.ONLY_MATCHING_FIELDS)));
  }

  @Test
  public void testCreateAsSelectAndLoad() throws IOException, InterruptedException {
    pyspark =
        makePysparkContainerWithDefaultConf(
            "testCreateAsSelectAndLoad", "/opt/spark_scripts/spark_ctas_load.py");
    pyspark.start();

    Path eventFolder = Paths.get("integrations/container/");

    String startCTASEvent = new String(readAllBytes(eventFolder.resolve("pysparkCTASStart.json")));
    String completeCTASEvent = new String(readAllBytes(eventFolder.resolve("pysparkCTASEnd.json")));

    String startLoadEvent = new String(readAllBytes(eventFolder.resolve("pysparkLoadStart.json")));
    String completeLoadEvent =
        new String(readAllBytes(eventFolder.resolve("pysparkLoadComplete.json")));

    mockServerClient.verify(
        request()
            .withPath("/api/v1/lineage")
            .withBody(json(startCTASEvent, MatchType.ONLY_MATCHING_FIELDS)),
        request()
            .withPath("/api/v1/lineage")
            .withBody(json(completeCTASEvent, MatchType.ONLY_MATCHING_FIELDS)),
        request()
            .withPath("/api/v1/lineage")
            .withBody(json(startLoadEvent, MatchType.ONLY_MATCHING_FIELDS)),
        request()
            .withPath("/api/v1/lineage")
            .withBody(json(completeLoadEvent, MatchType.ONLY_MATCHING_FIELDS)));
  }

  @Test
  @EnabledIfSystemProperty(
      named = "spark.version",
      matches = "(3.*)|(2\\.4\\.([8,9]|\\d\\d))") // Spark version >= 2.4.8
  public void testCTASDelta() throws IOException, InterruptedException {
    pyspark =
        makePysparkContainerWithDefaultConf(
            "testCTASDelta",
            "--packages",
            "io.delta:delta-core_2.12:1.0.0",
            "/opt/spark_scripts/spark_delta.py");
    pyspark.start();

    Path eventFolder = Paths.get("integrations/container/");

    String completeCTASEvent =
        new String(readAllBytes(eventFolder.resolve("pysparkDeltaCTASComplete.json")));

    mockServerClient.verify(
        request()
            .withPath("/api/v1/lineage")
            .withBody(json(completeCTASEvent, MatchType.ONLY_MATCHING_FIELDS)));
  }

  @Test
  public void testJDBCDataSource() throws IOException, InterruptedException {
    pyspark =
        makePysparkContainerWithDefaultConf(
            "testJdbcDataSource", "/opt/spark_scripts/spark_jdbc_datasource.py");
    pyspark.start();

    Path eventFolder = Paths.get("integrations/container/");

    String jdbcEnd = new String(readAllBytes(eventFolder.resolve("pysparkJDBCEnd.json")));

    mockServerClient.verify(
        request()
            .withPath("/api/v1/lineage")
            .withBody(json(jdbcEnd, MatchType.ONLY_MATCHING_FIELDS)));
  }

  @Test
  @EnabledIfSystemProperty(
      named = "spark.version",
      matches = "(3.*)|(2\\.4\\.([8,9]|\\d\\d))") // Spark version >= 2.4.8
  public void testOptimizedCreateAsSelectAndLoad() throws IOException, InterruptedException {
    pyspark =
        makePysparkContainerWithDefaultConf(
            "testOptimizedCreateAsSelectAndLoad", "/opt/spark_scripts/spark_octas_load.py");
    pyspark.start();

    Path eventFolder = Paths.get("integrations/container/");
    String startOCTASEvent =
        new String(readAllBytes(eventFolder.resolve("pysparkOCTASStart.json")));
    String completeOCTASEvent =
        new String(readAllBytes(eventFolder.resolve("pysparkOCTASEnd.json")));

    mockServerClient.verify(
        request()
            .withPath("/api/v1/lineage")
            .withBody(json(startOCTASEvent, MatchType.ONLY_MATCHING_FIELDS)),
        request()
            .withPath("/api/v1/lineage")
            .withBody(json(completeOCTASEvent, MatchType.ONLY_MATCHING_FIELDS)));
  }

  @Test
  public void testAlterTable() throws IOException {
    pyspark =
        makePysparkContainerWithDefaultConf(
            "testAlterTable", "/opt/spark_scripts/spark_alter_table.py");
    pyspark.start();

    Path eventFolder = Paths.get("integrations/container/");

    String completeAddColumnsEvent =
        new String(readAllBytes(eventFolder.resolve("pysparkAlterTableAddColumnsEnd.json")));
    String completeRenameEvent =
        new String(readAllBytes(eventFolder.resolve("pysparkAlterTableRenameEnd.json")));

    mockServerClient.verify(
        request()
            .withPath("/api/v1/lineage")
            .withBody(json(completeAddColumnsEvent, MatchType.ONLY_MATCHING_FIELDS)),
        request()
            .withPath("/api/v1/lineage")
            .withBody(json(completeRenameEvent, MatchType.ONLY_MATCHING_FIELDS)));
  }
}
