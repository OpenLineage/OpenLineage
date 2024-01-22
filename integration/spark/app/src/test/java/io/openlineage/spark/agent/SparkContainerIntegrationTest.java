/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent;

import com.google.common.collect.ImmutableMap;
import io.openlineage.spark.agent.olserver.OLServerClient;
import io.openlineage.spark.agent.olserver.OLServerContainer;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import lombok.SneakyThrows;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Tag("integration-test")
@Testcontainers
class SparkContainerIntegrationTest {

  private static final Network network = Network.newNetwork();

  @Container
  private static final OLServerContainer olServerMockContainer =
      SparkContainerUtils.makeOLServerContainer(network);

  private static final String SPARK_3 = "(3.*)";
  private static final String PACKAGES = "--packages";
  private static final String SPARK_VERSION = "spark.version";

  private static GenericContainer<?> pyspark;
  private static GenericContainer<?> kafka;
  private static OLServerClient olServerClient;
  private static final Logger logger = LoggerFactory.getLogger(SparkContainerIntegrationTest.class);

  @BeforeAll
  public static void setup() {
    olServerClient =
        new OLServerClient(olServerMockContainer.getHost(), olServerMockContainer.getPort());
    Awaitility.await().atMost(Duration.ofSeconds(20)).until(olServerMockContainer::isRunning);
  }

  @AfterEach
  public void cleanupSpark() {
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
      olServerMockContainer.stop();
    } catch (Exception e2) {
      logger.error("Unable to shut down openlineage client container", e2);
    }
    network.close();
  }

  @Test
  @EnabledIfSystemProperty(named = SPARK_VERSION, matches = SPARK_3) // Spark version >= 3.*
  void testPysparkWordCountWithCliArgs() {
    SparkContainerUtils.runPysparkContainerWithDefaultConf(
        network, "testPysparkWordCountWithCliArgs", "spark_word_count.py");
    olServerClient.verifyEvents(
        "pysparkWordCountWithCliArgsStartEvent.json",
        "pysparkWordCountWithCliArgsRunningEvent.json",
        "pysparkWordCountWithCliArgsCompleteEvent.json");
  }

  @Test
  @EnabledIfSystemProperty(named = SPARK_VERSION, matches = SPARK_3) // Spark version >= 3.*
  void testPysparkRddToTable() {
    SparkContainerUtils.runPysparkContainerWithDefaultConf(
        network, "testPysparkRddToTable", "spark_rdd_to_table.py");
    olServerClient.verifyEvents(
        "pysparkRddToCsvStartEvent.json",
        "pysparkRddToCsvCompleteEvent.json",
        "pysparkRddToTableStartEvent.json",
        "pysparkRddToTableRunningEvent.json",
        "pysparkRddToTableCompleteEvent.json");
  }

  @Test
  void testPysparkKafkaReadWrite() {
    kafka = SparkContainerUtils.makeKafkaContainer(network);
    kafka.start();

    pyspark =
        SparkContainerUtils.makePysparkContainerWithDefaultConf(
            network,
            "testPysparkKafkaReadWriteTest",
            PACKAGES,
            System.getProperty("kafka.package.version"),
            "/opt/spark_scripts/spark_kafka.py");
    pyspark.start();

    olServerClient.verifyEvents(
        "pysparkKafkaWriteStartEvent.json",
        "pysparkKafkaWriteCompleteEvent.json",
        "pysparkKafkaReadStartEvent.json",
        "pysparkKafkaReadCompleteEvent.json");
  }

  @Test
  @SneakyThrows
  void testPysparkKafkaReadAssign() {
    kafka = SparkContainerUtils.makeKafkaContainer(network);
    kafka.start();

    ImmutableMap<String, Object> kafkaProps =
        ImmutableMap.of(
            "bootstrap.servers",
            kafka.getHost() + ":" + kafka.getMappedPort(KafkaContainer.KAFKA_PORT));
    AdminClient admin = AdminClient.create(kafkaProps);
    CreateTopicsResult topicsResult =
        admin.createTopics(
            Arrays.asList(
                new NewTopic("topicA", 1, (short) 1), new NewTopic("topicB", 1, (short) 1)));
    topicsResult.topicId("topicA").get();

    SparkContainerUtils.runPysparkContainerWithDefaultConf(
        network, "testPysparkKafkaReadAssignTest", "spark_kafk_assign_read.py");

    olServerClient.verifyEvents(
        "pysparkKafkaAssignReadStartEvent.json", "pysparkKafkaAssignReadCompleteEvent.json");
  }

  @Test
  @EnabledIfSystemProperty(named = SPARK_VERSION, matches = SPARK_3) // Spark version >= 3.*
  void testPysparkSQLHiveTest() {
    SparkContainerUtils.runPysparkContainerWithDefaultConf(
        network, "testPysparkSQLHiveTest", "spark_hive.py");
    olServerClient.verifyEvents(
        "pysparkHiveStartEvent.json",
        "pysparkHiveRunningEvent.json",
        "pysparkHiveCompleteEvent.json",
        "pysparkHiveSelectStartEvent.json",
        "pysparkHiveSelectEndEvent.json");
  }

  @Test
  @EnabledIfSystemProperty(named = SPARK_VERSION, matches = SPARK_3) // Spark version >= 3.*
  void testOverwriteName() {
    SparkContainerUtils.runPysparkContainerWithDefaultConf(
        network,
        "testPysparkSQLHiveTest",
        Collections.singletonList("app_name=appName"),
        Collections.emptyList(),
        "overwrite_appname.py");
    olServerClient.verifyEvents(
        "pysparkOverwriteNameStartEvent.json",
        "pysparkOverwriteNameRunningEvent.json",
        "pysparkOverwriteNameCompleteEvent.json");
  }

  @Test
  @EnabledIfSystemProperty(named = SPARK_VERSION, matches = SPARK_3) // Spark version >= 3.*
  void testPysparkSQLOverwriteDirHiveTest() {
    SparkContainerUtils.runPysparkContainerWithDefaultConf(
        network, "testPysparkSQLHiveOverwriteDirTest", "spark_overwrite_hive.py");
    olServerClient.verifyEvents(
        "pysparkHiveOverwriteDirStartEvent.json", "pysparkHiveOverwriteDirCompleteEvent.json");
  }

  @Test
  void testCreateAsSelectAndLoad() {
    SparkContainerUtils.runPysparkContainerWithDefaultConf(
        network, "testCreateAsSelectAndLoad", "spark_ctas_load.py");
    olServerClient.verifyEvents(
        "pysparkCTASStart.json",
        "pysparkCTASEnd.json",
        "pysparkLoadStart.json",
        "pysparkLoadComplete.json");

    if (System.getProperty(SPARK_VERSION).matches(SPARK_3)) {
      // verify CTAS contains column level lineage
      olServerClient.verifyEvents("pysparkCTASWithColumnLineageEnd.json");
    }
  }

  @Test
  @EnabledIfSystemProperty(named = SPARK_VERSION, matches = SPARK_3) // Spark version >= 3.*
  void testCachedDataset() {
    SparkContainerUtils.runPysparkContainerWithDefaultConf(
        network, "cachedDataset", "spark_cached.py");
    olServerClient.verifyEvents("pysparkCachedDatasetComplete.json");
  }

  @Test
  @EnabledIfSystemProperty(named = SPARK_VERSION, matches = SPARK_3) // Spark version >= 3.*
  void testSymlinksFacetForHiveCatalog() {
    SparkContainerUtils.runPysparkContainerWithDefaultConf(
        network, "symlinks", "spark_hive_catalog.py");
    olServerClient.verifyEvents("pysparkSymlinksComplete.json");
  }

  @Test
  void testCreateTable() {
    SparkContainerUtils.runPysparkContainerWithDefaultConf(
        network, "testCreateTable", "spark_create_table.py");
    olServerClient.verifyEvents(
        "pysparkCreateTableStartEvent.json", "pysparkCreateTableCompleteEvent.json");
  }

  @Test
  void testDropTable() {
    SparkContainerUtils.runPysparkContainerWithDefaultConf(
        network, "testDropTable", "spark_drop_table.py");
    olServerClient.verifyEvents("pysparkDropTableCompleteEvent.json");
  }

  @Test
  void testTruncateTable() {
    SparkContainerUtils.runPysparkContainerWithDefaultConf(
        network, "testTruncateTable", "spark_truncate_table.py");
    olServerClient.verifyEvents("pysparkTruncateTableCompleteEvent.json");
  }

  @Test
  @EnabledIfSystemProperty(named = SPARK_VERSION, matches = SPARK_3)
  void testOptimizedCreateAsSelectAndLoad() {
    SparkContainerUtils.runPysparkContainerWithDefaultConf(
        network, "testOptimizedCreateAsSelectAndLoad", "spark_octas_load.py");
    olServerClient.verifyEvents("pysparkOCTASStart.json", "pysparkOCTASEnd.json");
  }

  @Test
  void testAlterTable() {
    SparkContainerUtils.runPysparkContainerWithDefaultConf(
        network, "testAlterTable", "spark_alter_table.py");
    olServerClient.verifyEvents(
        "pysparkAlterTableAddColumnsEnd.json", "pysparkAlterTableRenameEnd.json");
  }

  //  @Test
  //  @SneakyThrows
  //  void testFacetsDisable() {
  //    SparkContainerUtils.runPysparkContainerWithDefaultConf(
  //        network, "testFacetsDisable", "spark_facets_disable.py");
  //
  //    // response should not contain any of the below
  //    String regex = "^((?!(spark_unknown|spark.logicalPlan|dataSource)).)*$";
  //
  //    olServerClient.verify(request().withPath("/api/v1/lineage").withBody(new RegexBody(regex)));
  //  }

  @Test
  @SneakyThrows
  void testRddWithParquet() {
    SparkContainerUtils.runPysparkContainerWithDefaultConf(
        network, "testRddWithParquet", "spark_rdd_with_parquet.py");

    olServerClient.verifyEvents("pysparkRDDWithParquetComplete.json");
  }
}
