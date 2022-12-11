/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent;

import static java.nio.file.Files.readAllBytes;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.JsonBody.json;

import com.google.common.collect.ImmutableMap;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockserver.client.MockServerClient;
import org.mockserver.matchers.MatchType;
import org.mockserver.model.JsonBody;
import org.mockserver.model.RegexBody;
import org.mockserver.model.RequestDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.MockServerContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Tag("integration-test")
@Testcontainers
class SparkContainerIntegrationTest {

  private static final Network network = Network.newNetwork();

  @Container
  private static final MockServerContainer openLineageClientMockContainer =
      SparkContainerUtils.makeMockServerContainer(network);

  private static final String SPARK_3 = "(3.*)";
  private static final String PACKAGES = "--packages";
  private static final String SPARK_VERSION = "spark.version";

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

    Awaitility.await().until(openLineageClientMockContainer::isRunning);
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

  @Test
  @EnabledIfSystemProperty(named = SPARK_VERSION, matches = SPARK_3) // Spark version >= 3.*
  void testPysparkWordCountWithCliArgs() {
    SparkContainerUtils.runPysparkContainerWithDefaultConf(
        network,
        openLineageClientMockContainer,
        "testPysparkWordCountWithCliArgs",
        "spark_word_count.py");
    verifyEvents(
        "pysparkWordCountWithCliArgsStartEvent.json",
        "pysparkWordCountWithCliArgsCompleteEvent.json");
  }

  @Test
  @EnabledIfSystemProperty(named = SPARK_VERSION, matches = SPARK_3) // Spark version >= 3.*
  void testPysparkRddToTable() {
    SparkContainerUtils.runPysparkContainerWithDefaultConf(
        network, openLineageClientMockContainer, "testPysparkRddToTable", "spark_rdd_to_table.py");
    verifyEvents(
        "pysparkRddToCsvStartEvent.json",
        "pysparkRddToCsvCompleteEvent.json",
        "pysparkRddToTableStartEvent.json",
        "pysparkRddToTableCompleteEvent.json");
  }

  @Test
  void testPysparkKafkaReadWrite() {
    kafka = SparkContainerUtils.makeKafkaContainer(network);
    kafka.start();

    pyspark =
        SparkContainerUtils.makePysparkContainerWithDefaultConf(
            network,
            openLineageClientMockContainer,
            "testPysparkKafkaReadWriteTest",
            PACKAGES,
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
  void testPysparkKafkaReadAssign() {
    kafka = SparkContainerUtils.makeKafkaContainer(network);
    kafka.start();

    ImmutableMap<String, Object> kafkaProps =
        ImmutableMap.of(
            "bootstrap.servers",
            kafka.getHost() + ":" + kafka.getMappedPort(KafkaContainer.KAFKA_PORT));
    AdminClient admin = AdminClient.create(kafkaProps);
    admin.createTopics(
        Arrays.asList(new NewTopic("topicA", 1, (short) 0), new NewTopic("topicB", 1, (short) 0)));
    SparkContainerUtils.runPysparkContainerWithDefaultConf(
        network,
        openLineageClientMockContainer,
        "testPysparkKafkaReadAssignTest",
        "spark_kafk_assign_read.py");

    verifyEvents(
        "pysparkKafkaAssignReadStartEvent.json", "pysparkKafkaAssignReadCompleteEvent.json");
  }

  @Test
  @EnabledIfSystemProperty(named = SPARK_VERSION, matches = SPARK_3) // Spark version >= 3.*
  void testPysparkSQLHiveTest() {
    SparkContainerUtils.runPysparkContainerWithDefaultConf(
        network, openLineageClientMockContainer, "testPysparkSQLHiveTest", "spark_hive.py");
    verifyEvents(
        "pysparkHiveStartEvent.json",
        "pysparkHiveCompleteEvent.json",
        "pysparkHiveSelectStartEvent.json",
        "pysparkHiveSelectEndEvent.json");
  }

  @Test
  @EnabledIfSystemProperty(named = SPARK_VERSION, matches = SPARK_3) // Spark version >= 3.*
  void testOverwriteName() {
    SparkContainerUtils.runPysparkContainerWithDefaultConf(
        network,
        openLineageClientMockContainer,
        "testPysparkSQLHiveTest",
        Collections.singletonList("app_name=appName"),
        Collections.emptyList(),
        "overwrite_appname.py");
    verifyEvents("pysparkOverwriteNameStartEvent.json", "pysparkOverwriteNameCompleteEvent.json");
  }

  @Test
  @EnabledIfSystemProperty(named = SPARK_VERSION, matches = SPARK_3) // Spark version >= 3.*
  void testPysparkSQLOverwriteDirHiveTest() {
    SparkContainerUtils.runPysparkContainerWithDefaultConf(
        network,
        openLineageClientMockContainer,
        "testPysparkSQLHiveOverwriteDirTest",
        "spark_overwrite_hive.py");
    verifyEvents(
        "pysparkHiveOverwriteDirStartEvent.json", "pysparkHiveOverwriteDirCompleteEvent.json");
  }

  @Test
  void testCreateAsSelectAndLoad() {
    SparkContainerUtils.runPysparkContainerWithDefaultConf(
        network, openLineageClientMockContainer, "testCreateAsSelectAndLoad", "spark_ctas_load.py");
    verifyEvents(
        "pysparkCTASStart.json",
        "pysparkCTASEnd.json",
        "pysparkLoadStart.json",
        "pysparkLoadComplete.json");

    if (System.getProperty(SPARK_VERSION).matches(SPARK_3)) {
      // verify CTAS contains column level lineage
      verifyEvents("pysparkCTASWithColumnLineageEnd.json");
    }
  }

  @Test
  @EnabledIfSystemProperty(named = SPARK_VERSION, matches = SPARK_3) // Spark version >= 3.*
  void testCachedDataset() {
    SparkContainerUtils.runPysparkContainerWithDefaultConf(
        network, openLineageClientMockContainer, "cachedDataset", "spark_cached.py");
    verifyEvents("pysparkCachedDatasetComplete.json");
  }

  @Test
  @EnabledIfSystemProperty(named = SPARK_VERSION, matches = SPARK_3) // Spark version >= 3.*
  void testSymlinksFacetForHiveCatalog() {
    SparkContainerUtils.runPysparkContainerWithDefaultConf(
        network, openLineageClientMockContainer, "symlinks", "spark_hive_catalog.py");
    verifyEvents("pysparkSymlinksComplete.json");
  }

  @Test
  @EnabledIf("isDeltaTestEnabled")
  void testCTASDelta() {
    pyspark =
        SparkContainerUtils.makePysparkContainerWithDefaultConf(
            network,
            openLineageClientMockContainer,
            "testCTASDelta",
            PACKAGES,
            getDeltaPackageName(),
            "/opt/spark_scripts/spark_delta.py");
    pyspark.start();
    verifyEvents("pysparkDeltaCTASComplete.json");
  }

  @EnabledIf("isDeltaTestEnabled")
  @Test
  void testFilteringDeltaEvents() {
    pyspark =
        SparkContainerUtils.makePysparkContainerWithDefaultConf(
            network,
            openLineageClientMockContainer,
            "testV2Commands",
            PACKAGES,
            getDeltaPackageName(),
            "/opt/spark_scripts/spark_delta_event_filter.py");
    pyspark.start();
    assertEquals(
        12,
        mockServerClient.retrieveRecordedRequests(request().withPath("/api/v1/lineage")).length);
  }

  @Test
  @EnabledIf("isDeltaTestEnabled")
  void testDeltaSaveAsTable() {
    pyspark =
        SparkContainerUtils.makePysparkContainerWithDefaultConf(
            network,
            openLineageClientMockContainer,
            "testDeltaSaveAsTable",
            PACKAGES,
            getDeltaPackageName(),
            "/opt/spark_scripts/spark_delta_save_as_table.py");
    pyspark.start();
    verifyEvents("pysparkDeltaSaveAsTableComplete.json");
  }

  /*
   * Alter table differs between Spark 3.1 and Spark 3.2.
   * Delta support for 3.2 is also limited.
   * These test should only apply for Spark 3.1
   */
  @EnabledIfSystemProperty(named = SPARK_VERSION, matches = "(3.1.*)")
  @ParameterizedTest
  @CsvSource(
      value = {
        "spark_v2_alter.py:pysparkV2AlterTableStartEvent.json:pysparkV2AlterTableCompleteEvent.json:true",
        "spark_write_delta_table_version.py:pysparkWriteDeltaTableVersionStart.json:pysparkWriteDeltaTableVersionEnd.json:false"
      },
      delimiter = ':')
  void testAlterTableSpark_3_1(
      String pysparkScript,
      String expectedStartEvent,
      String expectedCompleteEvent,
      String isIceberg) {
    testV2Commands(pysparkScript, expectedStartEvent, expectedCompleteEvent, isIceberg);
  }

  @Test
  @EnabledIfSystemProperty(named = SPARK_VERSION, matches = "(3.2.*)")
  void testAlterTableSpark_3_2() {
    testV2Commands(
        "spark_v2_alter.py",
        "pysparkV2AlterTableStartEvent.json",
        "pysparkV2AlterTableCompleteEvent.json",
        "true");
  }

  @EnabledIfSystemProperty(named = SPARK_VERSION, matches = SPARK_3) // Spark version >= 3.*
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
        "spark_v2_append.py:pysparkV2AppendDataStartEvent.json:pysparkV2AppendDataCompleteEvent.json:true",
      },
      delimiter = ':')
  void testV2Commands(
      String pysparkScript,
      String expectedStartEvent,
      String expectedCompleteEvent,
      String isIceberg) {
    if (!Boolean.valueOf(isIceberg) && !isDeltaTestEnabled()) {
      // disable delta test for Spark 3.3 until delta starts supporting Spark 3.3
      return;
    }

    pyspark =
        SparkContainerUtils.makePysparkContainerWithDefaultConf(
            network,
            openLineageClientMockContainer,
            "testV2Commands",
            PACKAGES,
            Boolean.valueOf(isIceberg) ? getIcebergPackageName() : getDeltaPackageName(),
            "/opt/spark_scripts/" + pysparkScript);
    pyspark.start();
    verifyEvents(expectedStartEvent, expectedCompleteEvent);
  }

  private String getIcebergPackageName() {
    String sparkVersion = System.getProperty(SPARK_VERSION);
    if (sparkVersion.startsWith("3.1")) {
      return "org.apache.iceberg:iceberg-spark-runtime-3.1_2.12:0.13.0";
    } else if (sparkVersion.startsWith("3.2")) {
      return "org.apache.iceberg:iceberg-spark-runtime-3.2_2.12:0.13.0";
    } else if (sparkVersion.startsWith("3.3")) {
      return "org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:0.14.0";
    } else {
      // return previously used package name
      return "org.apache.iceberg:iceberg-spark3-runtime:0.12.0";
    }
  }

  private String getDeltaPackageName() {
    String sparkVersion = System.getProperty(SPARK_VERSION);
    if (sparkVersion.startsWith("3.2")) {
      return "io.delta:delta-core_2.12:1.1.0";
    } else if (sparkVersion.startsWith("3.3")) {
      return "io.delta:delta-core_2.12:2.1.0";
    }
    return "io.delta:delta-core_2.12:1.0.0";
  }

  @Test
  void testCreateTable() {
    SparkContainerUtils.runPysparkContainerWithDefaultConf(
        network, openLineageClientMockContainer, "testCreateTable", "spark_create_table.py");
    verifyEvents("pysparkCreateTableStartEvent.json", "pysparkCreateTableCompleteEvent.json");
  }

  @Test
  void testDropTable() {
    SparkContainerUtils.runPysparkContainerWithDefaultConf(
        network, openLineageClientMockContainer, "testDropTable", "spark_drop_table.py");
    verifyEvents("pysparkDropTableStartEvent.json");
  }

  @Test
  void testTruncateTable() {
    SparkContainerUtils.runPysparkContainerWithDefaultConf(
        network, openLineageClientMockContainer, "testTruncateTable", "spark_truncate_table.py");
    verifyEvents("pysparkTruncateTableStartEvent.json", "pysparkTruncateTableCompleteEvent.json");
  }

  @EnabledIf("isDeltaTestEnabled")
  @Test
  void testSaveIntoDataSourceCommand() {
    pyspark =
        SparkContainerUtils.makePysparkContainerWithDefaultConf(
            network,
            openLineageClientMockContainer,
            "testSaveIntoDataSource",
            PACKAGES,
            getDeltaPackageName(),
            "/opt/spark_scripts/spark_save_into_data_source.py");
    pyspark.start();
    verifyEvents("pysparkSaveIntoDatasourceCompleteEvent.json");
  }

  @Test
  @EnabledIfSystemProperty(named = SPARK_VERSION, matches = SPARK_3)
  void testOptimizedCreateAsSelectAndLoad() {
    SparkContainerUtils.runPysparkContainerWithDefaultConf(
        network,
        openLineageClientMockContainer,
        "testOptimizedCreateAsSelectAndLoad",
        "spark_octas_load.py");
    verifyEvents("pysparkOCTASStart.json", "pysparkOCTASEnd.json");
  }

  @Test
  @EnabledIfSystemProperty(named = SPARK_VERSION, matches = SPARK_3) // Spark version >= 3.*
  void testWriteIcebergTableVersion() {
    SparkContainerUtils.makePysparkContainerWithDefaultConf(
            network,
            openLineageClientMockContainer,
            "testWriteIcebergTableVersion",
            PACKAGES,
            getIcebergPackageName(),
            "/opt/spark_scripts/spark_write_iceberg_table_version.py")
        .start();
    verifyEvents("pysparkWriteIcebergTableVersionEnd.json");
  }

  @Test
  void testAlterTable() {
    SparkContainerUtils.runPysparkContainerWithDefaultConf(
        network, openLineageClientMockContainer, "testAlterTable", "spark_alter_table.py");
    verifyEvents("pysparkAlterTableAddColumnsEnd.json", "pysparkAlterTableRenameEnd.json");
  }

  @Test
  @EnabledIfEnvironmentVariable(named = "CI", matches = "true")
  @EnabledIfSystemProperty(
      named = SPARK_VERSION,
      matches = "(3.3.*)") // Only running on 3.3 because mockserver can't fill in the spark version
  void testReadAndWriteFromBigquery() {
    List<String> sparkConfigParams = new ArrayList<>();
    sparkConfigParams.add(
        "spark.hadoop.google.cloud.auth.service.account.json.keyfile=/opt/gcloud/gcloud-service-key.json");
    sparkConfigParams.add("spark.hadoop.google.cloud.auth.service.account.enable=true");
    sparkConfigParams.add(
        "spark.hadoop.fs.AbstractFileSystem.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS");
    sparkConfigParams.add(
        "spark.hadoop.fs.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem");

    SparkContainerUtils.runPysparkContainerWithDefaultConf(
        network,
        openLineageClientMockContainer,
        "testReadAndWriteFromBigquery",
        Collections.emptyList(),
        sparkConfigParams,
        "spark_bigquery.py");
    verifyEvents(
        "pysparkBigquerySaveStart.json",
        "pysparkBigqueryInsertStart.json",
        "pysparkBigqueryInsertStart.json",
        "pysparkBigqueryInsertEnd.json",
        "pysparkBigqueryInsertEnd.json",
        "pysparkBigquerySaveEnd.json");
  }

  @Test
  @SneakyThrows
  void testFacetsDisable() {
    SparkContainerUtils.runPysparkContainerWithDefaultConf(
        network, openLineageClientMockContainer, "testFacetsDisable", "spark_facets_disable.py");

    // response should not contain any of the below
    String regex = "^((?!(spark_unknown|spark.logicalPlan|dataSource)).)*$";

    mockServerClient.verify(request().withPath("/api/v1/lineage").withBody(new RegexBody(regex)));
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

  boolean isDeltaTestEnabled() {
    if (System.getProperty(SPARK_VERSION).startsWith("2")) {
      // we don't run integration tests for delta and Spark 2.x
      return false;
    }

    return true;
  }
}
