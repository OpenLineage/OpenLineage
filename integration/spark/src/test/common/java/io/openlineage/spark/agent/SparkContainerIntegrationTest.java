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
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.SneakyThrows;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockserver.client.MockServerClient;
import org.mockserver.matchers.MatchType;
import org.mockserver.model.JsonBody;
import org.mockserver.model.RequestDefinition;
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

  private static final String SPARK_3 = "(3.*)";
  private static final String SPARK_ABOVE_EQUAL_2_4_8 =
      "(3.*)|(2\\.4\\.([8,9]|\\d\\d))"; // Spark version >= 2.4.8

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
      if (pyspark != null) pyspark.stop();
    } catch (Exception e2) {
      logger.error("Unable to shut down pyspark container", e2);
    }
    try {
      if (kafka != null) kafka.stop();
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
                },
                command)
            .flatMap(Stream::of)
            .toArray(String[]::new));
  }

  private static void runPysparkContainerWithDefaultConf(String namespace, String pysparkFile) {
    makePysparkContainerWithDefaultConf(namespace, "/opt/spark_scripts/" + pysparkFile).start();
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
  public void testPysparkWordCountWithCliArgs() {
    runPysparkContainerWithDefaultConf("testPysparkWordCountWithCliArgs", "spark_word_count.py");
    verifyEvents(
        "pysparkWordCountWithCliArgsStartEvent.json",
        "pysparkWordCountWithCliArgsCompleteEvent.json");
  }

  @Test
  public void testPysparkRddToTable() {
    runPysparkContainerWithDefaultConf("testPysparkRddToTable", "spark_rdd_to_table.py");
    verifyEvents(
        "pysparkRddToCsvStartEvent.json",
        "pysparkRddToCsvCompleteEvent.json",
        "pysparkRddToTableStartEvent.json",
        "pysparkRddToTableCompleteEvent.json");
  }

  @Test
  public void testPysparkKafkaReadWrite() {
    kafka = makeKafkaContainer();
    kafka.start();

    pyspark =
        makePysparkContainerWithDefaultConf(
            "testPysparkKafkaReadWriteTest",
            "--packages",
            System.getProperty("kafka.package.version"),
            "/opt/spark_scripts/spark_kafka.py");
    pyspark.start();

    verifyEvents(
        "pysparkKafkaWriteStartEvent.json",
        "pysparkKafkaWriteCompleteEvent.json",
        "pysparkKafkaReadStartEvent.json",
        "pysparkKafkaReadCompleteEvent.json");
  }

  @Test
  public void testPysparkKafkaReadAssign() {
    kafka = makeKafkaContainer();
    kafka.start();

    ImmutableMap<String, Object> kafkaProps =
        ImmutableMap.of(
            "bootstrap.servers",
            kafka.getHost() + ":" + kafka.getMappedPort(KafkaContainer.KAFKA_PORT));
    AdminClient admin = AdminClient.create(kafkaProps);
    admin.createTopics(
        Arrays.asList(new NewTopic("topicA", 1, (short) 0), new NewTopic("topicB", 1, (short) 0)));
    runPysparkContainerWithDefaultConf(
        "testPysparkKafkaReadAssignTest", "spark_kafk_assign_read.py");

    verifyEvents(
        "pysparkKafkaAssignReadStartEvent.json", "pysparkKafkaAssignReadCompleteEvent.json");
  }

  @Test
  public void testPysparkSQLHiveTest() {
    runPysparkContainerWithDefaultConf("testPysparkSQLHiveTest", "spark_hive.py");
    verifyEvents("pysparkHiveStartEvent.json", "pysparkHiveCompleteEvent.json");
  }

  @Test
  public void testPysparkSQLOverwriteDirHiveTest() {
    runPysparkContainerWithDefaultConf(
        "testPysparkSQLHiveOverwriteDirTest", "spark_overwrite_hive.py");
    verifyEvents(
        "pysparkHiveOverwriteDirStartEvent.json", "pysparkHiveOverwriteDirCompleteEvent.json");
  }

  @Test
  public void testCreateAsSelectAndLoad() {
    runPysparkContainerWithDefaultConf("testCreateAsSelectAndLoad", "spark_ctas_load.py");
    verifyEvents(
        "pysparkCTASStart.json",
        "pysparkCTASEnd.json",
        "pysparkLoadStart.json",
        "pysparkLoadComplete.json");
  }

  @Test
  @EnabledIfSystemProperty(
      named = "spark.version",
      matches = SPARK_ABOVE_EQUAL_2_4_8) // Spark version >= 2.4.8
  public void testCTASDelta() {
    pyspark =
        makePysparkContainerWithDefaultConf(
            "testCTASDelta",
            "--packages",
            "io.delta:delta-core_2.12:1.0.0",
            "/opt/spark_scripts/spark_delta.py");
    pyspark.start();
    verifyEvents("pysparkDeltaCTASComplete.json");
  }

  @EnabledIfSystemProperty(named = "spark.version", matches = SPARK_3) // Spark version >= 3.*
  @ParameterizedTest
  @CsvSource(
      value = {
        "spark_v2_create.py:pysparkV2CreateTableStartEvent.json:pysparkV2CreateTableCompleteEvent.json:true",
        "spark_v2_create_as_select.py:pysparkV2CreateTableAsSelectStartEvent.json:pysparkV2CreateTableAsSelectCompleteEvent.json:true",
        "spark_v2_overwrite_by_expression.py:pysparkV2OverwriteByExpressionStartEvent.json:pysparkV2OverwriteByExpressionCompleteEvent.json:true",
        "spark_v2_overwrite_partitions.py:pysparkV2OverwritePartitionsStartEvent.json:pysparkV2OverwritePartitionsCompleteEvent.json:true",
        "spark_v2_replace_table_as_select.py:pysparkV2ReplaceTableAsSelectStartEvent.json:pysparkV2ReplaceTableAsSelectCompleteEvent.json:true",
        "spark_v2_replace_table.py:pysparkV2ReplaceTableStartEvent.json:pysparkV2ReplaceTableCompleteEvent.json:false",
        "spark_v2_delete.py:pysparkV2DeleteStartEvent.json:pysparkV2DeleteCompleteEvent.json:true",
        "spark_v2_update.py:pysparkV2UpdateStartEvent.json:pysparkV2UpdateCompleteEvent.json:true",
        "spark_v2_merge_into_table.py:pysparkV2MergeIntoTableStartEvent.json:pysparkV2MergeIntoTableCompleteEvent.json:true",
        "spark_v2_drop.py:pysparkV2DropTableStartEvent.json:pysparkV2DropTableCompleteEvent.json:true",
        "spark_v2_alter.py:pysparkV2AlterTableStartEvent.json:pysparkV2AlterTableCompleteEvent.json:true",
        "spark_v2_append.py:pysparkV2AppendDataStartEvent.json:pysparkV2AppendDataCompleteEvent.json:true"
      },
      delimiter = ':')
  public void testV2Commands(
      String pysparkScript,
      String expectedStartEvent,
      String expectedCompleteEvent,
      String isIceberg) {
    pyspark =
        makePysparkContainerWithDefaultConf(
            "testV2Commands",
            "--packages",
            Boolean.valueOf(isIceberg)
                ? "org.apache.iceberg:iceberg-spark3-runtime:0.12.0"
                : "io.delta:delta-core_2.12:1.0.0",
            "/opt/spark_scripts/" + pysparkScript);
    pyspark.start();
    verifyEvents(expectedStartEvent, expectedCompleteEvent);
  }

  @Test
  public void testCreateTable() {
    runPysparkContainerWithDefaultConf("testCreateTable", "spark_create_table.py");
    verifyEvents("pysparkCreateTableStartEvent.json", "pysparkCreateTableCompleteEvent.json");
  }

  @Test
  public void testDropTable() {
    runPysparkContainerWithDefaultConf("testDropTable", "spark_drop_table.py");
    verifyEvents("pysparkDropTableStartEvent.json");
  }

  @Test
  public void testTruncateTable() {
    runPysparkContainerWithDefaultConf("testTruncateTable", "spark_truncate_table.py");
    verifyEvents("pysparkTruncateTableStartEvent.json", "pysparkTruncateTableCompleteEvent.json");
  }

  @Test
  @EnabledIfSystemProperty(
      named = "spark.version",
      matches = SPARK_ABOVE_EQUAL_2_4_8) // Spark version >= 2.4.8
  public void testOptimizedCreateAsSelectAndLoad() {
    runPysparkContainerWithDefaultConf("testOptimizedCreateAsSelectAndLoad", "spark_octas_load.py");
    verifyEvents("pysparkOCTASStart.json", "pysparkOCTASEnd.json");
  }

  @Test
  @EnabledIfSystemProperty(named = "spark.version", matches = SPARK_3) // Spark version >= 3.*
  public void testWriteDeltaTableVersion() {
    makePysparkContainerWithDefaultConf(
            "testWriteDeltaTableVersion",
            "--packages",
            "io.delta:delta-core_2.12:1.0.0",
            "/opt/spark_scripts/spark_write_delta_table_version.py")
        .start();
    verifyEvents("pysparkWriteDeltaTableVersionEnd.json");
  }

  @Test
  @EnabledIfSystemProperty(named = "spark.version", matches = SPARK_3) // Spark version >= 3.*
  public void testWriteDeltaTableVersionDoesNotAttachToInput() {
    makePysparkContainerWithDefaultConf(
      "testWriteDeltaTableVersion",
      "--packages",
      "io.delta:delta-core_2.12:1.0.0",
      "/opt/spark_scripts/spark_write_delta_table_version_does_not_attach.py")
      .start();
    Assertions.assertThrows(
      AssertionError.class,
      () -> verifyEvents("pysparkWriteDeltaTableVersionDoesNotAttachToInputEnd.json")
    );
  }


  @Test
  @EnabledIfSystemProperty(named = "spark.version", matches = SPARK_3) // Spark version >= 3.*
  public void testWriteIcebergTableVersion() {
    makePysparkContainerWithDefaultConf(
            "testWriteIcebergTableVersion",
            "--packages",
            "org.apache.iceberg:iceberg-spark3-runtime:0.12.0",
            "/opt/spark_scripts/spark_write_iceberg_table_version.py")
        .start();
    verifyEvents("pysparkWriteIcebergTableVersionEnd.json");
  }

  @Test
  public void testAlterTable() {
    runPysparkContainerWithDefaultConf("testAlterTable", "spark_alter_table.py");
    verifyEvents("pysparkAlterTableAddColumnsEnd.json", "pysparkAlterTableRenameEnd.json");
  }

  private void verifyEvents(String... eventFiles) {
    Path eventFolder = Paths.get("integrations/container/");
    mockServerClient.verify(
        Arrays.stream(eventFiles)
            .map(
                fileEvent ->
                    request()
                        .withPath("/api/v1/lineage")
                        .withBody(readJson(eventFolder.resolve(fileEvent))))
            .collect(Collectors.toList())
            .toArray(new RequestDefinition[0]));
  }

  @SneakyThrows
  private JsonBody readJson(Path path) {
    return json(new String(readAllBytes(path)), MatchType.ONLY_MATCHING_FIELDS);
  }
}
