/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent;

import static io.openlineage.spark.agent.DatabricksUtils.DBFS_EVENTS_FILE;
import static io.openlineage.spark.agent.DatabricksUtils.init;
import static io.openlineage.spark.agent.DatabricksUtils.runScript;
import static org.assertj.core.api.Assertions.assertThat;

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.core.DatabricksConfig;
import io.openlineage.client.OpenLineage.InputDataset;
import io.openlineage.client.OpenLineage.OutputDataset;
import io.openlineage.client.OpenLineage.RunEvent;
import java.util.List;
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
public class DatabricksIntegrationTest {

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
  public void testCreateTableAsSelect() {
    List<RunEvent> runEvents = runScript(workspace, clusterId, "ctas.py");
    RunEvent lastEvent = runEvents.get(runEvents.size() - 1);

    OutputDataset outputDataset = lastEvent.getOutputs().get(0);
    InputDataset inputDataset = lastEvent.getInputs().get(0);

    assertThat(outputDataset.getNamespace()).isEqualTo("dbfs");
    assertThat(outputDataset.getName()).isEqualTo("/user/hive/warehouse/ctas");

    assertThat(inputDataset.getNamespace()).isEqualTo("dbfs");
    assertThat(inputDataset.getName()).isEqualTo("/user/hive/warehouse/temp");
  }
}
