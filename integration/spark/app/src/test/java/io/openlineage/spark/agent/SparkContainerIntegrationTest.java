/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent;

import static io.openlineage.spark.agent.MockServerUtils.verifyEvents;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockserver.model.HttpRequest.request;

import com.google.common.collect.ImmutableMap;
import io.openlineage.client.OpenLineage.InputDataset;
import io.openlineage.client.OpenLineage.InputStatisticsInputDatasetFacet;
import io.openlineage.client.OpenLineage.Job;
import io.openlineage.client.OpenLineage.OutputStatisticsOutputDatasetFacet;
import io.openlineage.client.OpenLineage.Run;
import io.openlineage.client.OpenLineage.RunEvent;
import io.openlineage.client.OpenLineage.RunEvent.EventType;
import io.openlineage.client.OpenLineage.RunFacet;
import io.openlineage.client.OpenLineage.RunFacets;
import io.openlineage.client.OpenLineageClientUtils;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
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
import org.mockserver.client.MockServerClient;
import org.mockserver.model.ClearType;
import org.mockserver.model.RegexBody;
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

  private static final String PACKAGES = "--packages";

  private static GenericContainer<?> pyspark;
  private static GenericContainer<?> kafka;
  private static MockServerClient mockServerClient;
  private static final Logger logger = LoggerFactory.getLogger(SparkContainerIntegrationTest.class);

  @BeforeAll
  public static void setupMockServer() {
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
  public void cleanupEverything() {
    mockServerClient.clear(request("/api/v1/lineage"), ClearType.LOG);
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
  public static void tearDownMockServer() {
    try {
      openLineageClientMockContainer.stop();
    } catch (Exception e2) {
      logger.error("Unable to shut down openlineage client container", e2);
    }
    network.close();
  }

  @Test
  void testPysparkWordCountWithCliArgs() {
    SparkContainerUtils.runPysparkContainerWithDefaultConf(
        network,
        openLineageClientMockContainer,
        "testPysparkWordCountWithCliArgs",
        "spark_word_count.py");
    verifyEvents(
        mockServerClient,
        "pysparkWordCountWithCliArgsStartEvent.json",
        "pysparkWordCountWithCliArgsRunningEvent.json",
        "pysparkWordCountWithCliArgsCompleteEvent.json");
  }

  @Test
  void testPysparkRddToTable() {
    SparkContainerUtils.runPysparkContainerWithDefaultConf(
        network, openLineageClientMockContainer, "testPysparkRddToTable", "spark_rdd_to_table.py");
    verifyEvents(
        mockServerClient,
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
            openLineageClientMockContainer,
            "testPysparkKafkaReadWriteTest",
            PACKAGES,
            System.getProperty("kafka.package.version"),
            "/opt/spark_scripts/spark_kafka.py");
    pyspark.start();

    verifyEvents(
        mockServerClient,
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
    topicsResult.all().get();

    SparkContainerUtils.runPysparkContainerWithDefaultConf(
        network,
        openLineageClientMockContainer,
        "testPysparkKafkaReadAssignTest",
        "spark_kafk_assign_read.py");

    verifyEvents(
        mockServerClient,
        "pysparkKafkaAssignReadStartEvent.json",
        "pysparkKafkaAssignReadCompleteEvent.json");
  }

  @Test
  void testPysparkSQLHiveTest() {
    SparkContainerUtils.runPysparkContainerWithDefaultConf(
        network, openLineageClientMockContainer, "testPysparkSQLHiveTest", "spark_hive.py");
    verifyEvents(
        mockServerClient,
        "pysparkHiveStartEvent.json",
        "pysparkHiveRunningEvent.json",
        "pysparkHiveCompleteEvent.json",
        "pysparkHiveSelectStartEvent.json",
        "pysparkHiveSelectEndEvent.json");
  }

  @Test
  void testPysparkSQLHadoopFSTest() {
    SparkContainerUtils.runPysparkContainerWithDefaultConf(
        network,
        openLineageClientMockContainer,
        "testPysparkSQLHadoopFSTest",
        "spark_hadoop_fs_relation.py");

    verifyEvents(
        mockServerClient, "pysparkHadoopFSStartEvent.json", "pysparkHadoopFSEndEvent.json");
  }

  @Test
  void testOverwriteName() {
    SparkContainerUtils.runPysparkContainerWithDefaultConf(
        network,
        openLineageClientMockContainer,
        "testPysparkSQLHiveTest",
        Collections.singletonList("app_name=appName"),
        Collections.emptyList(),
        "overwrite_appname.py");

    verifyEvents(
        mockServerClient,
        "pysparkOverwriteNameStartEvent.json",
        "pysparkOverwriteNameRunningEvent.json",
        "pysparkOverwriteNameCompleteEvent.json");
  }

  @Test
  void testPysparkSQLOverwriteDirHiveTest() {
    SparkContainerUtils.runPysparkContainerWithDefaultConf(
        network,
        openLineageClientMockContainer,
        "testPysparkSQLHiveOverwriteDirTest",
        "spark_overwrite_hive.py");
    verifyEvents(
        mockServerClient,
        "pysparkHiveOverwriteDirStartEvent.json",
        "pysparkHiveOverwriteDirCompleteEvent.json");
  }

  @Test
  void testCreateAsSelectAndLoad() {
    SparkContainerUtils.runPysparkContainerWithDefaultConf(
        network, openLineageClientMockContainer, "testCreateAsSelectAndLoad", "spark_ctas_load.py");
    verifyEvents(
        mockServerClient,
        "pysparkCTASStart.json",
        "pysparkCTASEnd.json",
        "pysparkLoadStart.json",
        "pysparkLoadComplete.json");

    verifyEvents(mockServerClient, "pysparkCTASWithColumnLineageEnd.json");
  }

  @Test
  void testCachedDataset() {
    SparkContainerUtils.runPysparkContainerWithDefaultConf(
        network, openLineageClientMockContainer, "cachedDataset", "spark_cached.py");
    verifyEvents(mockServerClient, "pysparkCachedDatasetComplete.json");
  }

  @Test
  void testSymlinksFacetForHiveCatalog() {
    SparkContainerUtils.runPysparkContainerWithDefaultConf(
        network, openLineageClientMockContainer, "symlinks", "spark_hive_catalog.py");
    verifyEvents(mockServerClient, "pysparkSymlinksComplete.json");
  }

  @Test
  void testCreateTable() {
    SparkContainerUtils.runPysparkContainerWithDefaultConf(
        network, openLineageClientMockContainer, "testCreateTable", "spark_create_table.py");
    verifyEvents(
        mockServerClient,
        "pysparkCreateTableStartEvent.json",
        "pysparkCreateTableCompleteEvent.json");

    // assert that START and COMPLETE events have the same job name with table name appended to it
    // we need to check it here as dataset names differ per Spark version (different default catalog
    // name)
    List<RunEvent> events =
        Arrays.stream(
                mockServerClient.retrieveRecordedRequests(request().withPath("/api/v1/lineage")))
            .map(r -> OpenLineageClientUtils.runEventFromJson(r.getBodyAsString()))
            .filter(e -> e.getJob().getName().endsWith("create_table_test"))
            .collect(Collectors.toList());

    assertThat(events.stream().map(e -> e.getJob().getName()).collect(Collectors.toList()))
        .containsOnly(events.get(0).getJob().getName()); // job name is same

    assertThat(events.get(0).getJob().getName())
        .endsWith("create_table_test"); // table name is appended to job name
  }

  @Test
  void testDropTable() {
    SparkContainerUtils.runPysparkContainerWithDefaultConf(
        network, openLineageClientMockContainer, "testDropTable", "spark_drop_table.py");
    verifyEvents(mockServerClient, "pysparkDropTableEvent.json");
  }

  @Test
  void testTruncateTable() {
    SparkContainerUtils.runPysparkContainerWithDefaultConf(
        network, openLineageClientMockContainer, "testTruncateTable", "spark_truncate_table.py");
    verifyEvents(mockServerClient, "pysparkTruncateTableCompleteEvent.json");
  }

  @Test
  void testOptimizedCreateAsSelectAndLoad() {
    SparkContainerUtils.runPysparkContainerWithDefaultConf(
        network,
        openLineageClientMockContainer,
        "testOptimizedCreateAsSelectAndLoad",
        "spark_octas_load.py");
    verifyEvents(mockServerClient, "pysparkOCTASStart.json", "pysparkOCTASEnd.json");
  }

  @Test
  void testColumnLevelLineage() {
    SparkContainerUtils.runPysparkContainerWithDefaultConf(
        network, openLineageClientMockContainer, "testColumnLevelLineage", "spark_cll.py");
    verifyEvents(mockServerClient, "pysparkCLLStart.json", "pysparkCLLEnd.json");
  }

  @Test
  void testAlterTable() {
    SparkContainerUtils.runPysparkContainerWithDefaultConf(
        network, openLineageClientMockContainer, "testAlterTable", "spark_alter_table.py");
    verifyEvents(
        mockServerClient, "pysparkAlterTableAddColumnsEnd.json", "pysparkAlterTableRenameEnd.json");
  }

  @Test
  @SneakyThrows
  @SuppressWarnings("PMD.JUnitTestsShouldIncludeAssert")
  void testFacetsDisable() {
    SparkContainerUtils.runPysparkContainerWithDefaultConf(
        network, openLineageClientMockContainer, "testFacetsDisable", "spark_facets_disable.py");

    // response should not contain any of the below
    String regex = "^((?!(spark_unknown|spark.logicalPlan|dataSource)).)*$";

    mockServerClient.verify(request().withPath("/api/v1/lineage").withBody(new RegexBody(regex)));
  }

  @Test
  @SneakyThrows
  void testRddRepartition() {
    SparkContainerUtils.runPysparkContainerWithDefaultConf(
        network, openLineageClientMockContainer, "tesInputOnlyRdd", "spark_input_only_rdd.py");

    List<RunEvent> events =
        Arrays.stream(
                mockServerClient.retrieveRecordedRequests(request().withPath("/api/v1/lineage")))
            .map(r -> OpenLineageClientUtils.runEventFromJson(r.getBodyAsString()))
            .filter(e -> e.getJob().getName().contains("map_partitions"))
            .collect(Collectors.toList());

    // there should be no outputs in all the events
    assertThat(events.stream().flatMap(e -> e.getOutputs().stream())).isEmpty();

    // input should point only to a known input
    assertThat(events.stream().flatMap(e -> e.getInputs().stream()).map(InputDataset::getName))
        .containsExactly("/tmp/input_rdd");
  }

  @Test
  @SneakyThrows
  void testRddWithParquet() {
    SparkContainerUtils.runPysparkContainerWithDefaultConf(
        network, openLineageClientMockContainer, "testRddWithParquet", "spark_rdd_with_parquet.py");

    verifyEvents(mockServerClient, "pysparkRDDWithParquet.json");

    // verify content of output statistics facet
    Optional<OutputStatisticsOutputDatasetFacet> outputStatisticsFacet =
        Arrays.stream(
                mockServerClient.retrieveRecordedRequests(request().withPath("/api/v1/lineage")))
            .map(r -> OpenLineageClientUtils.runEventFromJson(r.getBodyAsString()))
            .flatMap(r -> r.getOutputs().stream())
            .filter(d -> d.getName().contains("rdd_c"))
            .filter(d -> d.getOutputFacets().getOutputStatistics() != null)
            .map(d -> d.getOutputFacets().getOutputStatistics())
            .findFirst();
    assertThat(outputStatisticsFacet.isPresent()).isTrue();
    assertThat(outputStatisticsFacet.get().getRowCount()).isEqualTo(4L);

    // verify content of output statistics facet
    Optional<InputStatisticsInputDatasetFacet> inputStatisticsFacet =
        Arrays.stream(
                mockServerClient.retrieveRecordedRequests(request().withPath("/api/v1/lineage")))
            .map(r -> OpenLineageClientUtils.runEventFromJson(r.getBodyAsString()))
            .flatMap(r -> r.getInputs().stream())
            .filter(d -> d.getName().contains("rdd_c"))
            .filter(d -> d.getInputFacets().getInputStatistics() != null)
            .map(d -> d.getInputFacets().getInputStatistics())
            .findAny();
    // there are two input datasets, task based input metrics should not be used to obtain input
    // size
    assertThat(inputStatisticsFacet).isEmpty();
  }

  @Test
  @SneakyThrows
  void testSingleRddStatistics() {
    SparkContainerUtils.runPysparkContainerWithDefaultConf(
        network,
        openLineageClientMockContainer,
        "testSingleRddInputStats",
        "spark_single_input_rdd.py");

    verifyEvents(mockServerClient, "pysparkSingleRDDStatistics.json");
  }

  @Test
  void testDebugFacet() {
    SparkContainerUtils.runPysparkContainerWithDefaultConf(
        network,
        openLineageClientMockContainer,
        "testEmitMetrics",
        Collections.emptyList(),
        Collections.singletonList("spark.openlineage.facets.debug.disabled=false"),
        "spark_emit_metrics.py");
    verifyEvents(mockServerClient, "pysparkMetricsEnd.json");
  }

  @Test
  @EnabledIfSystemProperty(named = "spark.version", matches = "([34].*)")
  void testSmartDebugFacet() {
    SparkContainerUtils.runPysparkContainerWithDefaultConf(
        network,
        openLineageClientMockContainer,
        "testSmartDebugFacet",
        Collections.emptyList(),
        Arrays.asList(
            "spark.openlineage.debug.smart=true",
            "spark.openlineage.debug.smartMode=output-missing",
            "spark.openlineage.filter.allowedSparkNodes=[org.apache.spark.sql.catalyst.plans.logical.ShowTables]", // get smart debug facet only for ShowTables node
            "spark.openlineage.facets.debug.disabled=true" // make sure debug facet is disabled
            ),
        "spark_smart_debug.py");

    List<RunEvent> events =
        Arrays.stream(
                mockServerClient.retrieveRecordedRequests(request().withPath("/api/v1/lineage")))
            .map(r -> OpenLineageClientUtils.runEventFromJson(r.getBodyAsString()))
            .collect(Collectors.toList());

    // make sure application event does not have debug facet
    assertThat(
            events.stream()
                .map(r -> r.getRun().getFacets())
                .filter(r -> r.getParent() == null)
                .filter(r -> r.getAdditionalProperties().containsKey("debug")))
        .isEmpty();

    // make sure debug facet comes only for COMPLETE event
    assertThat(
            events.stream()
                .filter(r -> r.getRun().getFacets().getAdditionalProperties().containsKey("debug"))
                .map(RunEvent::getEventType)
                .collect(Collectors.toSet()))
        .containsOnly(EventType.COMPLETE);

    // assert job name of the event with debug facet
    assertThat(
            events.stream()
                .filter(r -> r.getRun().getFacets().getAdditionalProperties().containsKey("debug"))
                .map(RunEvent::getJob)
                .map(Job::getName)
                .collect(Collectors.toSet()))
        .containsAnyOf(
            "smart_debug_facet_test.show_tables",
            "smart_debug_facet_test.execute_show_tables_command");

    // assert there is only one debug facet in the events received
    List<RunFacet> debugFacets =
        events.stream()
            .map(RunEvent::getRun)
            .map(Run::getFacets)
            .map(RunFacets::getAdditionalProperties)
            .filter(p -> p.containsKey("debug"))
            .map(p -> p.get("debug"))
            .collect(Collectors.toList());

    // make sure debug facet is included only once and this happens for COMPLETE event
    assertThat(debugFacets).hasSize(1);
    assertThat((List<String>) debugFacets.get(0).getAdditionalProperties().get("logs"))
        .containsExactly("No input datasets detected", "No output datasets detected");
  }
}
