/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent;

import static io.openlineage.spark.agent.MockServerUtils.getEventsEmitted;
import static org.assertj.core.api.Assertions.assertThat;

import io.openlineage.client.OpenLineage.OneLakeDatasetFacet;
import io.openlineage.client.OpenLineage.OutputDataset;
import io.openlineage.client.OpenLineage.RunEvent;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockserver.integration.ClientAndServer;

/**
 * Microsoft's Spark connector for Fabric warehouses ({@code synapsesql}) isn't publicly available,
 * so this test repeats what it does inside Spark: it saves Parquet files to a temporary folder with
 * options that name the real table, in a session with the settings Fabric provides. The connector's
 * second step, loading the files with {@code COPY INTO}, runs outside Spark, so OpenLineage never
 * sees it.
 */
@Tag("integration-test")
class SparkFabricWarehouseIntegrationTest {
  @SuppressWarnings("PMD")
  private static final String LOCAL_IP = "127.0.0.1";

  private static final int MOCK_SERVER_PORT = 1087;
  private static final String STAGING_ROOT = "/tmp/fabric_warehouse_it/trusted-service-user";
  private static final String TENANT = "68a4b712-9eec-4ca0-a2d3-6a96d2cf4f9c";
  private static final String WORKSPACE = "d1f6473b-9f7a-4352-940b-a8d74461d3fc";
  private static final String WORKSPACE_2 = "45ff911e-5e75-428a-a618-52958fcc7604";
  private static final String WAREHOUSE_ID = "d75873bd-716b-4378-a37e-864c76fc9b3b";
  private static final String TABLE = "wh_primary.probe_dw.a_write";
  private static final String ENDPOINT_PREFIX = "fabric-warehouse://ck32i2hmt2qeziwtnklnft2ptq-";
  private static final String ENDPOINT_SUFFIX = ".datawarehouse.fabric.microsoft.com";

  private static ClientAndServer mockServer;
  private SparkSession spark;

  @BeforeAll
  static void beforeAll() {
    Spark4CompatUtils.cleanupAnyExistingSession();
    mockServer = MockServerUtils.createAndConfigureMockServer(MOCK_SERVER_PORT);
  }

  @AfterAll
  static void afterAll() {
    Spark4CompatUtils.cleanupAnyExistingSession();
    MockServerUtils.stopMockServer(mockServer);
  }

  @BeforeEach
  void beforeEach() {
    MockServerUtils.clearRequests(mockServer);
    spark =
        SparkSession.builder()
            .master("local[*]")
            .appName("FabricWarehouseIntegrationTest")
            .config("spark.driver.host", LOCAL_IP)
            .config("spark.driver.bindAddress", LOCAL_IP)
            .config("spark.sql.shuffle.partitions", 1)
            .config("spark.openlineage.transport.type", "http")
            .config(
                "spark.openlineage.transport.url",
                "http://localhost:" + mockServer.getPort() + "/api/v1/lineage")
            .config("spark.openlineage.namespace", "fabric-namespace")
            .config("spark.extraListeners", OpenLineageSparkListener.class.getName())
            // Fabric sets these in every notebook
            .config("trident.tenant.id", TENANT)
            .config("trident.workspace.id", WORKSPACE)
            .config("trident.workspace.name", "do-test-env-fabric")
            .getOrCreate();
  }

  @Test
  void connectorWriteEmitsWarehouseTableNotStagingFolder() {
    stagingWrite("a_write5f15", WORKSPACE, WAREHOUSE_ID);
    spark.stop();

    List<OutputDataset> outputs = outputs(getEventsEmitted(mockServer));

    assertThat(outputs).isNotEmpty();
    assertThat(outputs)
        .allSatisfy(
            output -> {
              assertThat(output.getNamespace())
                  .isEqualTo(ENDPOINT_PREFIX + "hnd7nul2t5jehfalvdluiyot7q" + ENDPOINT_SUFFIX);
              assertThat(output.getName()).isEqualTo(TABLE);
              assertThat(output.getFacets().getLifecycleStateChange()).isNull();
              assertThat(output.getFacets().getSymlinks().getIdentifiers())
                  .singleElement()
                  .satisfies(
                      symlink -> {
                        assertThat(symlink.getNamespace())
                            .isEqualTo(
                                "abfss://" + WORKSPACE + "@onelake.dfs.fabric.microsoft.com");
                        assertThat(symlink.getName())
                            .isEqualTo(WAREHOUSE_ID + "/Tables/probe_dw/a_write");
                        assertThat(symlink.getType()).isEqualTo("LOCATION");
                      });
            });
  }

  @Test
  void connectorWriteSetsOneLakeFacet() {
    stagingWrite("a_write9d41", WORKSPACE, WAREHOUSE_ID);
    spark.stop();

    List<OutputDataset> outputs = outputs(getEventsEmitted(mockServer));

    assertThat(outputs).isNotEmpty();
    assertThat(outputs)
        .allSatisfy(
            output -> {
              OneLakeDatasetFacet oneLake = output.getFacets().getOneLake();
              assertThat(oneLake.getItemType()).isEqualTo("Warehouse");
              assertThat(oneLake.getItemName()).isEqualTo("wh_primary");
              assertThat(oneLake.getWorkspaceId()).isEqualTo(WORKSPACE);
              assertThat(oneLake.getWorkspaceName()).isEqualTo("do-test-env-fabric");
              assertThat(oneLake.getItemId()).isEqualTo(WAREHOUSE_ID);
              assertThat(oneLake.getLocation())
                  .isEqualTo(
                      "abfss://"
                          + WORKSPACE
                          + "@onelake.dfs.fabric.microsoft.com/"
                          + WAREHOUSE_ID
                          + "/Tables/probe_dw/a_write");
            });
  }

  @Test
  void crossWorkspaceWriteUsesWorkspaceOption() {
    stagingWrite("a_write7c2e", WORKSPACE_2, null);
    spark.stop();

    List<OutputDataset> outputs = outputs(getEventsEmitted(mockServer));

    assertThat(outputs).isNotEmpty();
    assertThat(outputs)
        .allSatisfy(
            output -> {
              assertThat(output.getNamespace())
                  .isEqualTo(ENDPOINT_PREFIX + "d2i76rlvl2fefjqykkky7tdwaq" + ENDPOINT_SUFFIX);
              assertThat(output.getFacets().getSymlinks()).isNull();
              // another workspace than the session's: no name, and no warehouse ID was given
              OneLakeDatasetFacet oneLake = output.getFacets().getOneLake();
              assertThat(oneLake.getWorkspaceId()).isEqualTo(WORKSPACE_2);
              assertThat(oneLake.getWorkspaceName()).isNull();
              assertThat(oneLake.getItemName()).isEqualTo("wh_primary");
              assertThat(oneLake.getItemId()).isNull();
              assertThat(oneLake.getLocation()).isNull();
            });
  }

  @Test
  void plainParquetWriteIsUnchanged() {
    data().write().mode("overwrite").parquet(STAGING_ROOT + "/plain_write");
    spark.stop();

    List<OutputDataset> outputs = outputs(getEventsEmitted(mockServer));

    assertThat(outputs).isNotEmpty();
    assertThat(outputs)
        .allSatisfy(
            output -> {
              assertThat(output.getNamespace()).isEqualTo("file");
              assertThat(output.getName()).isEqualTo(STAGING_ROOT + "/plain_write");
              assertThat(output.getFacets().getOneLake()).isNull();
            });
  }

  // the connector's first write step, with the options seen on Fabric Runtime 2.0
  @SneakyThrows
  private void stagingWrite(String folder, String workspaceId, String warehouseId) {
    org.apache.spark.sql.DataFrameWriter<Row> writer =
        data()
            .write()
            .mode("overwrite")
            .option("WorkspaceId", workspaceId)
            .option("three-part_sql_table_or_view_name", TABLE);
    if (warehouseId != null) {
      writer = writer.option("DatawarehouseId", warehouseId);
    }
    writer.parquet(STAGING_ROOT + "/" + folder);
  }

  private Dataset<Row> data() {
    StructType schema =
        new StructType().add("id", DataTypes.IntegerType).add("v", DataTypes.StringType);
    return spark.createDataFrame(
        Arrays.asList(RowFactory.create(1, "a"), RowFactory.create(2, "b")), schema);
  }

  private static List<OutputDataset> outputs(List<RunEvent> events) {
    return events.stream()
        .flatMap(event -> event.getOutputs().stream())
        .collect(Collectors.toList());
  }
}
