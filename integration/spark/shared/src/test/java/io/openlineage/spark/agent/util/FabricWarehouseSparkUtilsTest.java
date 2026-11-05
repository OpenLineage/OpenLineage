/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.util;

import static org.assertj.core.api.Assertions.assertThat;

import io.openlineage.client.utils.DatasetIdentifier;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import org.junit.jupiter.api.Test;

@SuppressWarnings("PMD.AvoidDuplicateLiterals")
class FabricWarehouseSparkUtilsTest {
  private static final String TENANT = "68a4b712-9eec-4ca0-a2d3-6a96d2cf4f9c";
  private static final String WORKSPACE = "d1f6473b-9f7a-4352-940b-a8d74461d3fc";
  private static final String WORKSPACE_2 = "45ff911e-5e75-428a-a618-52958fcc7604";
  private static final String WAREHOUSE_ID = "d75873bd-716b-4378-a37e-864c76fc9b3b";
  private static final String ENDPOINT =
      "ck32i2hmt2qeziwtnklnft2ptq-hnd7nul2t5jehfalvdluiyot7q.datawarehouse.fabric.microsoft.com";
  private static final String ENDPOINT_2 =
      "ck32i2hmt2qeziwtnklnft2ptq-d2i76rlvl2fefjqykkky7tdwaq.datawarehouse.fabric.microsoft.com";
  private static final String TABLE = "wh_primary.probe_dw.a_write";
  // the address of a workspace reached as a guest from another organization has a third part
  private static final String GUEST_ENDPOINT =
      "45jm47djf4je5afhvapdos5fae-gakpgz3u6twe7nemz77to4q2ti-jh0bi.zcf.datawarehouse.fabric.microsoft.com";

  private final Map<String, String> conf = new HashMap<>();
  private final Function<String, Optional<String>> confLookup =
      key -> Optional.ofNullable(conf.get(key));

  @Test
  void testEndpointOverrideWins() {
    conf.put(
        "spark.datawarehouse.wh_primary.sqlendpoint",
        "ck32i2hmt2qeziwtnklnft2ptq-hnd7nul2t5jehfalvdluiyot7q.z8b.datawarehouse.fabric.microsoft.com,1433");
    conf.put("trident.tenant.id", TENANT);
    Map<String, String> options = options(TABLE);
    options.put("WorkspaceId", WORKSPACE_2);

    assertThat(identifier(options).getNamespace()).isEqualTo("fabric-warehouse://" + ENDPOINT);
  }

  @Test
  void testEndpointFromWorkspaceOption() {
    // writing to another workspace: the option names the workspace being written to, while
    // trident.workspace.id is the notebook's own workspace
    conf.put("trident.tenant.id", TENANT);
    conf.put("trident.workspace.id", WORKSPACE);
    Map<String, String> options = options(TABLE);
    options.put("WorkspaceId", WORKSPACE_2);

    DatasetIdentifier identifier = identifier(options);

    assertThat(identifier.getNamespace()).isEqualTo("fabric-warehouse://" + ENDPOINT_2);
    assertThat(identifier.getName()).isEqualTo(TABLE);
  }

  @Test
  void testEndpointFromTridentConfOnly() {
    conf.put("trident.tenant.id", TENANT);
    conf.put("trident.workspace.id", WORKSPACE);

    assertThat(identifier(options(TABLE)).getNamespace())
        .isEqualTo("fabric-warehouse://" + ENDPOINT);
  }

  @Test
  void testNoOutputWithoutWorkspace() {
    conf.put("trident.tenant.id", TENANT);

    assertThat(FabricWarehouseSparkUtils.isConnectorWrite(options(TABLE))).isTrue();
    assertThat(FabricWarehouseSparkUtils.getWriteIdentifier(options(TABLE), confLookup)).isEmpty();
  }

  @Test
  void testNoOutputWithoutTenant() {
    conf.put("trident.workspace.id", WORKSPACE);

    assertThat(FabricWarehouseSparkUtils.getWriteIdentifier(options(TABLE), confLookup)).isEmpty();
  }

  @Test
  void testMixedCaseOptionKeys() {
    conf.put("trident.tenant.id", TENANT);
    Map<String, String> options = new HashMap<>();
    options.put("THREE-PART_SQL_TABLE_OR_VIEW_NAME", TABLE);
    options.put("workspaceid", WORKSPACE);
    options.put("datawarehouseid", WAREHOUSE_ID);

    DatasetIdentifier identifier = identifier(options);

    assertThat(identifier.getNamespace()).isEqualTo("fabric-warehouse://" + ENDPOINT);
    assertThat(identifier.getSymlinks()).hasSize(1);
  }

  @Test
  void testOneLakeLocationOnlyWithDatawarehouseId() {
    conf.put("trident.tenant.id", TENANT);
    Map<String, String> options = options(TABLE);
    options.put("WorkspaceId", WORKSPACE);
    assertThat(identifier(options).getSymlinks()).isEmpty();

    options.put("DatawarehouseId", WAREHOUSE_ID);
    assertThat(identifier(options).getSymlinks())
        .containsExactly(
            new DatasetIdentifier.Symlink(
                WAREHOUSE_ID + "/Tables/probe_dw/a_write",
                "abfss://" + WORKSPACE + "@onelake.dfs.fabric.microsoft.com",
                DatasetIdentifier.SymlinkType.LOCATION));
  }

  @Test
  void testNotAConnectorWrite() {
    Map<String, String> options = new HashMap<>();
    options.put("path", "abfss://container@account.dfs.core.windows.net/data");
    assertThat(FabricWarehouseSparkUtils.isConnectorWrite(options)).isFalse();

    // the table must be named <warehouse>.<schema>.<table>
    assertThat(FabricWarehouseSparkUtils.isConnectorWrite(options("probe_dw.a_write"))).isFalse();
  }

  @Test
  void testGuestAddressSettingEmitsNoOutput() {
    // the Fabric settings would give an address, but the setting is the one that was used
    conf.put("trident.tenant.id", TENANT);
    conf.put("trident.workspace.id", WORKSPACE);
    conf.put("spark.datawarehouse.wh_primary.sqlendpoint", GUEST_ENDPOINT);

    assertThat(FabricWarehouseSparkUtils.isConnectorWrite(options(TABLE))).isTrue();
    assertThat(FabricWarehouseSparkUtils.getWriteIdentifier(options(TABLE), confLookup)).isEmpty();
  }

  @Test
  void testMalformedPortInSettingEmitsNoOutput() {
    conf.put("trident.tenant.id", TENANT);
    conf.put("trident.workspace.id", WORKSPACE);
    conf.put("spark.datawarehouse.wh_primary.sqlendpoint", ENDPOINT + ":bad");

    assertThat(FabricWarehouseSparkUtils.getWriteIdentifier(options(TABLE), confLookup)).isEmpty();
  }

  @Test
  void testBlankSettingIsIgnored() {
    conf.put("trident.tenant.id", TENANT);
    conf.put("trident.workspace.id", WORKSPACE);
    conf.put("spark.datawarehouse.wh_primary.sqlendpoint", "  ");

    assertThat(identifier(options(TABLE)).getNamespace())
        .isEqualTo("fabric-warehouse://" + ENDPOINT);
  }

  private DatasetIdentifier identifier(Map<String, String> options) {
    return FabricWarehouseSparkUtils.getWriteIdentifier(options, confLookup).get();
  }

  private static Map<String, String> options(String table) {
    Map<String, String> options = new HashMap<>();
    options.put("three-part_sql_table_or_view_name", table);
    options.put(
        "path",
        "abfss://e5c0cf65-0000-0000-0000-000000000000@spark1triprodcus.dfs.core.windows.net"
            + "/user/trusted-service-user/a_write5f15");
    return options;
  }
}
