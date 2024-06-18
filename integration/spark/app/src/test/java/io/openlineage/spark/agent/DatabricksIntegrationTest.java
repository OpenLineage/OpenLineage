/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent;

import static io.openlineage.spark.agent.DatabricksUtils.DBFS_EVENTS_FILE;
import static io.openlineage.spark.agent.DatabricksUtils.init;
import static io.openlineage.spark.agent.DatabricksUtils.platformVersion;
import static io.openlineage.spark.agent.DatabricksUtils.runScript;
import static org.assertj.core.api.Assertions.assertThat;

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.core.DatabricksConfig;
import io.openlineage.client.OpenLineage.ColumnLineageDatasetFacetFieldsAdditional;
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
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Integration tests to check Openlineage Spark integration on a databricks platform. This test
 * takes some time as it requires starting databricks cluster and this can take up to 10 minutes.
 * Test suite is run in Circle CI when triggered manually with extra parameter `databricks-test` set
 * to `enabled`.
 */
@Tag("integration-test")
@Tag("databricks")
@Slf4j
@SuppressWarnings("PMD.AvoidDuplicateLiterals")
class DatabricksIntegrationTest {

  private static WorkspaceClient workspace;
  private static String clusterId;

  @BeforeAll
  @SneakyThrows
  public static void setup() {
    DatabricksConfig config =
        new DatabricksConfig()
            .setHost(System.getProperty("databricksHost"))
            .setToken(System.getProperty("databricksToken"));

    workspace = new WorkspaceClient(config);
    clusterId = init(workspace);
  }

  @BeforeEach
  public void beforeEach() {
    workspace.dbfs().delete(DBFS_EVENTS_FILE);
  }

  @AfterAll
  public static void shutdown() {
    if (clusterId != null) {
      DatabricksUtils.shutdown(workspace, clusterId);
    }
  }

  @Test
  @SneakyThrows
  void testCreateTableAsSelect() {
    List<RunEvent> runEvents = runScript(workspace, clusterId, "ctas.py");
    RunEvent lastEvent = runEvents.get(runEvents.size() - 1);

    OutputDataset outputDataset = lastEvent.getOutputs().get(0);
    InputDataset inputDataset = lastEvent.getInputs().get(0);

    assertThat(outputDataset.getNamespace()).isEqualTo("dbfs");
    assertThat(outputDataset.getName()).isEqualTo("/user/hive/warehouse/ctas_" + platformVersion());

    assertThat(inputDataset.getNamespace()).isEqualTo("dbfs");
    assertThat(inputDataset.getName()).isEqualTo("/user/hive/warehouse/temp_" + platformVersion());

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
    List<RunEvent> runEvents = runScript(workspace, clusterId, "narrow_transformation.py");
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
        .isEqualTo("/data/path/to/output/narrow_transformation_" + platformVersion());
  }

  @Test
  @SneakyThrows
  void testWideTransformation() {
    List<RunEvent> runEvents = runScript(workspace, clusterId, "wide_transformation.py");
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
        .isEqualTo("/data/output/wide_transformation/result_" + platformVersion());
  }

  @Test
  void testWriteReadFromTableWithLocation() {
    List<RunEvent> runEvents = runScript(workspace, clusterId, "dataset_names.py");

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
    List<RunEvent> runEvents = runScript(workspace, clusterId, "merge_into.py");

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
    assertThat(event.getOutputs().get(0).getName()).endsWith("events_" + platformVersion());

    assertThat(event.getInputs()).hasSize(2);
    assertThat(event.getInputs().stream().map(d -> d.getName()).collect(Collectors.toList()))
        .containsExactlyInAnyOrder(
            "/user/hive/warehouse/test_db.db/updates_" + platformVersion(),
            "/user/hive/warehouse/test_db.db/events_" + platformVersion());

    assertThat(fields).hasSize(2);
    assertThat(fields.get("last_updated_at").getInputFields()).hasSize(1);
    assertThat(fields.get("last_updated_at").getInputFields().get(0))
        .hasFieldOrPropertyWithValue("namespace", "dbfs")
        .hasFieldOrPropertyWithValue(
            "name", "/user/hive/warehouse/test_db.db/updates_" + platformVersion())
        .hasFieldOrPropertyWithValue("field", "updated_at");

    assertThat(fields.get("event_id").getInputFields()).hasSize(2);
    assertThat(
            fields.get("event_id").getInputFields().stream()
                .filter(e -> e.getName().endsWith("updates_" + platformVersion()))
                .findFirst()
                .get())
        .hasFieldOrPropertyWithValue("namespace", "dbfs")
        .hasFieldOrPropertyWithValue(
            "name", "/user/hive/warehouse/test_db.db/updates_" + platformVersion())
        .hasFieldOrPropertyWithValue("field", "event_id");

    assertThat(
            fields.get("event_id").getInputFields().stream()
                .filter(e -> e.getName().endsWith("events_" + platformVersion()))
                .findFirst()
                .get())
        .hasFieldOrPropertyWithValue("namespace", "dbfs")
        .hasFieldOrPropertyWithValue(
            "name", "/user/hive/warehouse/test_db.db/events_" + platformVersion())
        .hasFieldOrPropertyWithValue("field", "event_id");
  }
}
