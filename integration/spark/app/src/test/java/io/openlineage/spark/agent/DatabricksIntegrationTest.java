/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent;

import static org.assertj.core.api.Assertions.assertThat;

import io.openlineage.client.OpenLineage.ColumnLineageDatasetFacetFieldsAdditional;
import io.openlineage.client.OpenLineage.DatasetFacets;
import io.openlineage.client.OpenLineage.InputDataset;
import io.openlineage.client.OpenLineage.OutputDataset;
import io.openlineage.client.OpenLineage.RunEvent;
import io.openlineage.client.OpenLineage.RunEvent.EventType;
import io.openlineage.client.OpenLineage.RunFacet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Integration tests to check Openlineage Spark integration on a databricks platform. This test
 * takes some time as it requires starting databricks cluster and this can take up to 10 minutes.
 * Test suite is run in Circle CI when triggered manually with extra parameter `databricks-test` set
 * to `enabled`.
 *
 * <p>By default the tests use a pre-created cluster: the only one named {@code openlineage-test...}
 * whose Databricks runtime matches the Spark and Scala versions of the build (see {@link
 * DatabricksEnvironment#DATABRICKS_RUNTIMES}). The cluster is restarted so that it loads the
 * freshly built jar. The cluster must be pinned (so it stays listed while terminated), use the init
 * script {@value DatabricksEnvironment#INIT_SCRIPT_FILE}, log to {@value
 * DatabricksEnvironment#DBFS_CLUSTER_LOGS}, run in single user mode with the CI principal as the
 * user, and the CI principal needs CAN_RESTART on it. The tests fail if no such cluster is found.
 */
@Tag("integration-test")
@Tag("databricks")
@Slf4j
@SuppressWarnings("PMD.AvoidDuplicateLiterals")
class DatabricksIntegrationTest {

  private static DatabricksEnvironment databricks;
  private static String platformVersion;

  @BeforeAll
  static void beforeAll() {
    databricks =
        new DatabricksEnvironment(
            DatabricksEnvironment.DatabricksEnvironmentProperties.builder()
                .workspace(
                    DatabricksEnvironment.DatabricksEnvironmentProperties.Workspace.builder()
                        .host(DatabricksDynamicParameter.Host.resolve())
                        .token(DatabricksDynamicParameter.Token.resolve())
                        .build())
                .cluster(
                    DatabricksEnvironment.DatabricksEnvironmentProperties.Cluster.builder()
                        .sparkVersion(DatabricksDynamicParameter.SparkVersion.resolve())
                        .scalaVersion(DatabricksDynamicParameter.ScalaVersion.resolve())
                        .build())
                .development(
                    DatabricksEnvironment.DatabricksEnvironmentProperties.Development.builder()
                        .existingClusterId(DatabricksDynamicParameter.ClusterId.resolve())
                        .createCluster(
                            Boolean.parseBoolean(
                                DatabricksDynamicParameter.CreateCluster.resolve()))
                        .restartExistingCluster(
                            Boolean.parseBoolean(
                                DatabricksDynamicParameter.RestartExistingCluster.resolve()))
                        .preventClusterTermination(
                            Boolean.parseBoolean(
                                DatabricksDynamicParameter.PreventClusterTermination.resolve()))
                        .fetchLog4jLogs(
                            Boolean.parseBoolean(
                                DatabricksDynamicParameter.FetchLog4jLogs.resolve()))
                        .log4jLogsLocation(DatabricksDynamicParameter.Log4jLogsLocation.resolve())
                        .fetchStdout(
                            Boolean.parseBoolean(DatabricksDynamicParameter.FetchStdout.resolve()))
                        .stdoutLocation(DatabricksDynamicParameter.StdoutLocation.resolve())
                        .fetchStderr(
                            Boolean.parseBoolean(DatabricksDynamicParameter.FetchStderr.resolve()))
                        .stderrLocation(DatabricksDynamicParameter.StderrLocation.resolve())
                        .fetchEvents(
                            Boolean.parseBoolean(DatabricksDynamicParameter.FetchEvents.resolve()))
                        .eventsFileLocation(DatabricksDynamicParameter.EventsFileLocation.resolve())
                        .build())
                .build());
    platformVersion = databricks.getPlatformVersion();
  }

  @BeforeEach
  public void beforeEach() {
    databricks.deleteEventsFile();
  }

  /**
   * Fails the test if the cluster didn't start. It's checked per test instead of throwing in {@link
   * #beforeAll()}, so the resources are still cleaned and the cluster logs collected.
   */
  private void requireClusterRunning() {
    assertThat(databricks.isInitializationSuccessful())
        .as("The Databricks cluster failed to start, see the log above")
        .isTrue();
  }

  @AfterAll
  public static void shutdown() {
    // null when beforeAll failed, e.g. no matching cluster was found
    if (databricks == null) {
      return;
    }
    databricks.fetchLogs();
    databricks.close();
  }

  @Test
  @SneakyThrows
  void testCreateTableAsSelect() {
    requireClusterRunning();

    List<RunEvent> runEvents = databricks.runScript("ctas.py");
    RunEvent lastEvent = runEvents.get(runEvents.size() - 1);

    OutputDataset outputDataset = lastEvent.getOutputs().get(0);
    InputDataset inputDataset = lastEvent.getInputs().get(0);

    assertThat(outputDataset.getNamespace()).isEqualTo("dbfs");
    assertThat(outputDataset.getName()).isEqualTo("/user/hive/warehouse/ctas_" + platformVersion);

    assertThat(inputDataset.getNamespace()).isEqualTo("dbfs");
    assertThat(inputDataset.getName()).isEqualTo("/user/hive/warehouse/temp_" + platformVersion);

    // test DatabricksEnvironmentFacetBuilder handler
    RunEvent eventWithDatabricksProperties =
        runEvents.stream()
            .filter(
                r ->
                    r.getRun()
                        .getFacets()
                        .getAdditionalProperties()
                        .containsKey("environment-properties"))
            .findFirst()
            .get();

    RunFacet environmentFacet =
        eventWithDatabricksProperties
            .getRun()
            .getFacets()
            .getAdditionalProperties()
            .get("environment-properties");

    Map<String, Object> properties =
        (Map<String, Object>)
            environmentFacet.getAdditionalProperties().get("environment-properties");

    assertThat(properties.get("spark.databricks.job.type")).isEqualTo("python");

    List<Object> mounts = (List<Object>) properties.get("mountPoints");

    assertThat(mounts).isNotEmpty();
    Map<String, String> mountInfo = (Map<String, String>) mounts.get(0);

    assertThat(mountInfo).containsKeys("mountPoint", "source");

    assertThat(mountInfo.get("mountPoint")).startsWith("/databricks");
    assertThat(mountInfo.get("source")).startsWith("databricks");
  }

  @Test
  @SneakyThrows
  void testNarrowTransformation() {
    requireClusterRunning();

    List<RunEvent> runEvents = databricks.runScript("narrow_transformation.py");
    assertThat(runEvents).isNotEmpty();

    // assert start event exists
    assertThat(
            runEvents.stream()
                .filter(
                    s ->
                        s.getJob()
                            .getName()
                            .contains("execute_insert_into_hadoop_fs_relation_command"))
                .filter(s -> s.getEventType().equals(EventType.START))
                .findFirst())
        .isPresent();

    // assert complete event contains output dataset
    Optional<RunEvent> completeEvent =
        runEvents.stream()
            .filter(
                s ->
                    s.getJob().getName().contains("execute_insert_into_hadoop_fs_relation_command"))
            .filter(s -> s.getEventType().equals(EventType.COMPLETE))
            .findFirst();

    assertThat(completeEvent).isPresent();
    assertThat(completeEvent.get().getOutputs().get(0).getName())
        .isEqualTo("/data/path/to/output/narrow_transformation_" + platformVersion);
  }

  @Test
  @SneakyThrows
  void testWideTransformation() {
    requireClusterRunning();

    List<RunEvent> runEvents = databricks.runScript("wide_transformation.py");
    assertThat(runEvents).isNotEmpty();

    // assert start event exists
    assertThat(
            runEvents.stream()
                .filter(s -> s.getJob().getName().contains("adaptive_spark_plan"))
                .filter(s -> s.getEventType().equals(EventType.START))
                .findFirst())
        .isPresent();

    // assert complete event contains output dataset
    Optional<RunEvent> completeEvent =
        runEvents.stream()
            .filter(s -> s.getJob().getName().contains("adaptive_spark_plan"))
            .filter(s -> s.getEventType().equals(EventType.COMPLETE))
            .findFirst();

    assertThat(completeEvent).isPresent();
    assertThat(completeEvent.get().getOutputs().get(0).getName())
        .isEqualTo("/data/output/wide_transformation/result_" + platformVersion);
  }

  @Test
  void testWriteReadFromTableWithLocation() {
    requireClusterRunning();

    List<RunEvent> runEvents = databricks.runScript("dataset_names.py");

    // find complete event with output dataset containing name
    OutputDataset outputDataset =
        runEvents.stream()
            .filter(runEvent -> runEvent.getOutputs().size() > 0)
            .filter(runEvent -> runEvent.getOutputs().get(0).getName().contains("t1"))
            .map(runEvent -> runEvent.getOutputs().get(0))
            .findFirst()
            .get();

    // find start event with input dataset containing name
    InputDataset inputDataset =
        runEvents.stream()
            .filter(runEvent -> runEvent.getInputs().size() > 0)
            .filter(runEvent -> runEvent.getInputs().get(0).getName().contains("t1"))
            .map(runEvent -> runEvent.getInputs().get(0))
            .findFirst()
            .get();

    // assert input and output are the same
    assertThat(inputDataset.getNamespace()).isEqualTo(outputDataset.getNamespace());
    assertThat(inputDataset.getName()).isEqualTo(outputDataset.getName());
    assertThat(runEvents.size()).isLessThan(20);
  }

  @Test
  @SneakyThrows
  void testMergeInto() {
    requireClusterRunning();

    List<RunEvent> runEvents = databricks.runScript("merge_into.py");

    RunEvent event =
        runEvents.stream()
            .filter(runEvent -> runEvent.getOutputs().size() > 0)
            .filter(runEvent -> runEvent.getOutputs().get(0).getFacets() != null)
            .filter(runEvent -> runEvent.getOutputs().get(0).getFacets().getColumnLineage() != null)
            .findFirst()
            .get();

    Map<String, ColumnLineageDatasetFacetFieldsAdditional> fields =
        event
            .getOutputs()
            .get(0)
            .getFacets()
            .getColumnLineage()
            .getFields()
            .getAdditionalProperties();

    assertThat(event.getOutputs()).hasSize(1);
    assertThat(event.getOutputs().get(0).getName()).endsWith("events_" + platformVersion);

    assertThat(event.getInputs()).hasSize(2);
    assertThat(event.getInputs().stream().map(d -> d.getName()).collect(Collectors.toList()))
        .containsExactlyInAnyOrder(
            "/user/hive/warehouse/test_db.db/updates_" + platformVersion,
            "/user/hive/warehouse/test_db.db/events_" + platformVersion);

    assertThat(fields).hasSize(2);
    assertThat(fields.get("last_updated_at").getInputFields()).hasSize(1);
    assertThat(fields.get("last_updated_at").getInputFields().get(0))
        .hasFieldOrPropertyWithValue("namespace", "dbfs")
        .hasFieldOrPropertyWithValue(
            "name", "/user/hive/warehouse/test_db.db/updates_" + platformVersion)
        .hasFieldOrPropertyWithValue("field", "updated_at");

    assertThat(fields.get("event_id").getInputFields()).hasSize(2);
    assertThat(
            fields.get("event_id").getInputFields().stream()
                .filter(e -> e.getName().endsWith("updates_" + platformVersion))
                .findFirst()
                .get())
        .hasFieldOrPropertyWithValue("namespace", "dbfs")
        .hasFieldOrPropertyWithValue(
            "name", "/user/hive/warehouse/test_db.db/updates_" + platformVersion)
        .hasFieldOrPropertyWithValue("field", "event_id");

    assertThat(
            fields.get("event_id").getInputFields().stream()
                .filter(e -> e.getName().endsWith("events_" + platformVersion))
                .findFirst()
                .get())
        .hasFieldOrPropertyWithValue("namespace", "dbfs")
        .hasFieldOrPropertyWithValue(
            "name", "/user/hive/warehouse/test_db.db/events_" + platformVersion)
        .hasFieldOrPropertyWithValue("field", "event_id");
  }

  @Test
  @Disabled("Unity Catalog is not available in the CI environment")
  // If enabling the test, make sure to provide a valid catalog name both here and in
  // unity_catalog.py
  void testUnityCatalog() {
    String catalogName = "PROVIDE_NAME";
    requireClusterRunning();

    List<RunEvent> runEvents = databricks.runScript("unity_catalog.py");

    Optional<RunEvent> completeEvent =
        runEvents.stream()
            .filter(
                s -> s.getJob().getName().contains("atomic_create_table_as_select.default_tbl2"))
            .filter(s -> s.getEventType().equals(EventType.COMPLETE))
            .findFirst();
    assertThat(completeEvent).isPresent();

    verifyCatalogRelatedFacets(completeEvent.get().getInputs().get(0).getFacets(), catalogName);
    verifyCatalogRelatedFacets(completeEvent.get().getOutputs().get(0).getFacets(), catalogName);
  }

  private static void verifyCatalogRelatedFacets(DatasetFacets dataset, String catalogName) {
    assertThat(dataset.getCatalog()).isNotNull();
    assertThat(dataset.getCatalog().getType()).isEqualTo("unity");
    assertThat(dataset.getCatalog().getName()).isEqualTo(catalogName);

    assertThat(dataset.getStorage()).isNotNull();
    assertThat(dataset.getStorage().getStorageLayer()).isEqualTo("unity");

    assertThat(dataset.getSymlinks()).isNotNull();
  }
}
