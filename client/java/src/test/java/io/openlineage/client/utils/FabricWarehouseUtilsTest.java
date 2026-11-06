/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.utils;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

// The IDs and addresses below come from real Fabric workspaces. They check that the address is
// built exactly as Fabric builds it; see FabricWarehouseUtils#endpointFor.
@SuppressWarnings("PMD.AvoidDuplicateLiterals")
class FabricWarehouseUtilsTest {
  private static final UUID TENANT = UUID.fromString("68a4b712-9eec-4ca0-a2d3-6a96d2cf4f9c");
  private static final UUID WORKSPACE = UUID.fromString("d1f6473b-9f7a-4352-940b-a8d74461d3fc");
  private static final UUID WORKSPACE_2 = UUID.fromString("45ff911e-5e75-428a-a618-52958fcc7604");
  private static final UUID WAREHOUSE = UUID.fromString("d75873bd-716b-4378-a37e-864c76fc9b3b");
  private static final String ENDPOINT =
      "ck32i2hmt2qeziwtnklnft2ptq-hnd7nul2t5jehfalvdluiyot7q.datawarehouse.fabric.microsoft.com";
  private static final String ENDPOINT_2 =
      "ck32i2hmt2qeziwtnklnft2ptq-d2i76rlvl2fefjqykkky7tdwaq.datawarehouse.fabric.microsoft.com";

  @Test
  void testEndpointFor() {
    assertThat(FabricWarehouseUtils.endpointFor(TENANT, WORKSPACE)).isEqualTo(ENDPOINT);
  }

  @Test
  void testEncodeGuid() {
    assertThat(FabricWarehouseUtils.encodeGuid(WORKSPACE_2))
        .isEqualTo("d2i76rlvl2fefjqykkky7tdwaq");
  }

  @Test
  void testDecodeGuidRoundTrip() {
    assertThat(FabricWarehouseUtils.decodeGuid("d2i76rlvl2fefjqykkky7tdwaq")).contains(WORKSPACE_2);
    assertThat(FabricWarehouseUtils.decodeGuid(FabricWarehouseUtils.encodeGuid(TENANT)))
        .contains(TENANT);
  }

  @Test
  void testDecodeGuidRejectsMalformedLabels() {
    assertThat(FabricWarehouseUtils.decodeGuid("d2i76rlvl2fefjqykkky7tdwa")).isEmpty();
    assertThat(FabricWarehouseUtils.decodeGuid("d2i76rlvl2fefjqykkky7tdwa1")).isEmpty();
    // the last character leaves extra bits that aren't zero, so it can't come from an ID
    assertThat(FabricWarehouseUtils.decodeGuid("d2i76rlvl2fefjqykkky7tdwar")).isEmpty();
  }

  @ParameterizedTest
  @CsvSource(
      delimiter = '|',
      value = {
        "ck32i2hmt2qeziwtnklnft2ptq-hnd7nul2t5jehfalvdluiyot7q.z8b.datawarehouse.fabric.microsoft.com,1433"
            + " | "
            + ENDPOINT,
        "ck32i2hmt2qeziwtnklnft2ptq-d2i76rlvl2fefjqykkky7tdwaq.zbc.datawarehouse.fabric.microsoft.com:1433"
            + " | "
            + ENDPOINT_2,
        ENDPOINT + ":1444 | " + ENDPOINT + ":1444",
        ENDPOINT + ",1444 | " + ENDPOINT + ":1444",
        "CK32I2HMT2QEZIWTNKLNFT2PTQ-HND7NUL2T5JEHFALVDLUIYOT7Q.DATAWAREHOUSE.FABRIC.MICROSOFT.COM | "
            + ENDPOINT,
        // a ".z8b"-style label is only removed right before ".datawarehouse..."
        "a.z8b.example.datawarehouse.fabric.microsoft.com | a.z8b.example.datawarehouse.fabric.microsoft.com",
        "myserver.database.windows.net:1433 | myserver.database.windows.net",
      })
  void testCanonicalEndpoint(String endpoint, String expected) {
    assertThat(FabricWarehouseUtils.canonicalEndpoint(endpoint)).isEqualTo(expected);
  }

  @ParameterizedTest
  @CsvSource(
      delimiter = '|',
      value = {
        ENDPOINT + " | true",
        ENDPOINT + ":1444 | true",
        ENDPOINT + ",1433 | true",
        // a port has to be a number
        ENDPOINT + ":bad | false",
        ENDPOINT + ": | false",
        "ck32i2hmt2qeziwtnklnft2ptq-hnd7nul2t5jehfalvdluiyot7q.z8b.datawarehouse.fabric.microsoft.com:1433 | true",
        // an address for reaching a workspace in another organization has an extra part
        "45jm47djf4je5afhvapdos5fae-gakpgz3u6twe7nemz77to4q2ti-jh0bi.zcf.datawarehouse.fabric.microsoft.com | false",
        "myserver.database.windows.net | false",
        "myworkspace.datawarehouse.fabric.microsoft.com | false",
      })
  void testIsFabricEndpoint(String endpoint, boolean expected) {
    assertThat(FabricWarehouseUtils.isFabricEndpoint(endpoint)).isEqualTo(expected);
  }

  @Test
  void testWorkspaceIdOf() {
    assertThat(
            FabricWarehouseUtils.workspaceIdOf(
                "ck32i2hmt2qeziwtnklnft2ptq-d2i76rlvl2fefjqykkky7tdwaq.zbc.datawarehouse.fabric.microsoft.com"))
        .contains(WORKSPACE_2);
    assertThat(FabricWarehouseUtils.workspaceIdOf("myserver.database.windows.net")).isEmpty();
  }

  @Test
  void testOneLakeLocation() {
    UUID warehouse = UUID.fromString("d75873bd-716b-4378-a37e-864c76fc9b3b");

    DatasetIdentifier location =
        FabricWarehouseUtils.oneLakeLocation(WORKSPACE, warehouse, "raw", "orders");

    assertThat(location.getNamespace())
        .isEqualTo("abfss://d1f6473b-9f7a-4352-940b-a8d74461d3fc@onelake.dfs.fabric.microsoft.com");
    assertThat(location.getName())
        .isEqualTo("d75873bd-716b-4378-a37e-864c76fc9b3b/Tables/raw/orders");
  }

  @Test
  void testWithOneLakeLocationFromWorkspaceOption() {
    DatasetIdentifier identifier =
        FabricWarehouseUtils.withOneLakeLocation(
            new DatasetIdentifier("wh_primary.raw.orders", "fabric-warehouse://" + ENDPOINT),
            Optional.of("wh_primary"),
            Optional.of(WORKSPACE_2.toString()),
            Optional.of(WAREHOUSE.toString()));

    assertThat(identifier.getSymlinks())
        .containsExactly(
            new DatasetIdentifier.Symlink(
                WAREHOUSE + "/Tables/raw/orders",
                "abfss://" + WORKSPACE_2 + "@onelake.dfs.fabric.microsoft.com",
                DatasetIdentifier.SymlinkType.LOCATION));
  }

  @Test
  void testWithOneLakeLocationDecodesWorkspaceFromEndpoint() {
    DatasetIdentifier identifier =
        FabricWarehouseUtils.withOneLakeLocation(
            new DatasetIdentifier("wh_primary.raw.orders", "fabric-warehouse://" + ENDPOINT),
            Optional.of("wh_primary"),
            Optional.empty(),
            Optional.of(WAREHOUSE.toString()));

    assertThat(identifier.getSymlinks())
        .extracting(DatasetIdentifier.Symlink::getNamespace)
        .containsExactly("abfss://" + WORKSPACE + "@onelake.dfs.fabric.microsoft.com");
  }

  @Test
  void testWithOneLakeLocationSkippedWhenUnsafe() {
    String namespace = "fabric-warehouse://" + ENDPOINT;
    Optional<String> warehouse = Optional.of(WAREHOUSE.toString());

    // no warehouse ID
    assertThat(
            FabricWarehouseUtils.withOneLakeLocation(
                    new DatasetIdentifier("wh_primary.raw.orders", namespace),
                    Optional.of("wh_primary"),
                    Optional.empty(),
                    Optional.empty())
                .getSymlinks())
        .isEmpty();
    // a name instead of an ID would give a path that mixes IDs and names, which OneLake refuses
    assertThat(
            FabricWarehouseUtils.withOneLakeLocation(
                    new DatasetIdentifier("wh_primary.raw.orders", namespace),
                    Optional.of("wh_primary"),
                    Optional.of("my workspace"),
                    warehouse)
                .getSymlinks())
        .isEmpty();
    // not a <warehouse>.<schema>.<table> name
    assertThat(
            FabricWarehouseUtils.withOneLakeLocation(
                    new DatasetIdentifier("raw.orders", namespace),
                    Optional.of("raw"),
                    Optional.empty(),
                    warehouse)
                .getSymlinks())
        .isEmpty();
    // not a Fabric dataset
    assertThat(
            FabricWarehouseUtils.withOneLakeLocation(
                    new DatasetIdentifier("sales.dbo.orders", "sqlserver://myserver:1433"),
                    Optional.of("sales"),
                    Optional.of(WORKSPACE.toString()),
                    warehouse)
                .getSymlinks())
        .isEmpty();
  }

  @Test
  void testWithOneLakeLocationOnlyForTheIdsWarehouse() {
    String namespace = "fabric-warehouse://" + ENDPOINT;
    Optional<String> workspace = Optional.of(WORKSPACE.toString());
    Optional<String> warehouse = Optional.of(WAREHOUSE.toString());

    // a query in wh_primary can also read wh_secondary, but the ID is wh_primary's
    assertThat(
            FabricWarehouseUtils.withOneLakeLocation(
                    new DatasetIdentifier("wh_secondary.probe_pr.l9_other", namespace),
                    Optional.of("wh_primary"),
                    workspace,
                    warehouse)
                .getSymlinks())
        .isEmpty();
    // warehouse names don't depend on upper or lower case
    assertThat(
            FabricWarehouseUtils.withOneLakeLocation(
                    new DatasetIdentifier("WH_Primary.raw.orders", namespace),
                    Optional.of("wh_primary"),
                    workspace,
                    warehouse)
                .getSymlinks())
        .hasSize(1);
    // without knowing which warehouse the ID is for, no location is added
    assertThat(
            FabricWarehouseUtils.withOneLakeLocation(
                    new DatasetIdentifier("wh_primary.raw.orders", namespace),
                    Optional.empty(),
                    workspace,
                    warehouse)
                .getSymlinks())
        .isEmpty();
  }

  @Test
  void testToOneLakeLocationWithWarehouseId() {
    DatasetIdentifier identifier =
        FabricWarehouseUtils.withOneLakeLocation(
            new DatasetIdentifier("wh_primary.raw.orders", "fabric-warehouse://" + ENDPOINT),
            Optional.of("wh_primary"),
            Optional.empty(),
            Optional.of(WAREHOUSE.toString()));

    OneLakeLocation location = FabricWarehouseUtils.toOneLakeLocation(identifier).get();

    assertThat(location.getWorkspaceId()).isEqualTo(WORKSPACE);
    assertThat(location.getItemId()).isEqualTo(WAREHOUSE);
    assertThat(location.getItemName()).isEqualTo("wh_primary");
    assertThat(location.getItemType()).isEqualTo("Warehouse");
    assertThat(location.guidLocation())
        .contains(
            "abfss://"
                + WORKSPACE
                + "@onelake.dfs.fabric.microsoft.com/"
                + WAREHOUSE
                + "/Tables/raw/orders");
  }

  @Test
  void testToOneLakeLocationWithoutWarehouseId() {
    OneLakeLocation location =
        FabricWarehouseUtils.toOneLakeLocation(
                new DatasetIdentifier("wh_primary.raw.orders", "fabric-warehouse://" + ENDPOINT))
            .get();

    assertThat(location.getWorkspaceId()).isEqualTo(WORKSPACE);
    assertThat(location.getItemName()).isEqualTo("wh_primary");
    assertThat(location.getItemType()).isEqualTo("Warehouse");
    assertThat(location.getItemId()).isNull();
    assertThat(location.guidLocation()).isEmpty();
  }

  @Test
  void testToOneLakeLocationIgnoresLocationInAnotherWorkspace() {
    // the WorkspaceId option names another workspace than the address; the address wins, and the
    // location, which belongs to the other workspace, isn't used
    DatasetIdentifier identifier =
        FabricWarehouseUtils.withOneLakeLocation(
            new DatasetIdentifier("wh_primary.raw.orders", "fabric-warehouse://" + ENDPOINT),
            Optional.of("wh_primary"),
            Optional.of(WORKSPACE_2.toString()),
            Optional.of(WAREHOUSE.toString()));

    OneLakeLocation location = FabricWarehouseUtils.toOneLakeLocation(identifier).get();

    assertThat(location.getWorkspaceId()).isEqualTo(WORKSPACE);
    assertThat(location.getItemId()).isNull();
  }

  @Test
  void testToOneLakeLocationOnlyForFabricWarehouseTables() {
    assertThat(
            FabricWarehouseUtils.toOneLakeLocation(
                new DatasetIdentifier("db.dbo.orders", "sqlserver://myserver:1433")))
        .isEmpty();
    assertThat(
            FabricWarehouseUtils.toOneLakeLocation(
                new DatasetIdentifier("raw.orders", "fabric-warehouse://" + ENDPOINT)))
        .isEmpty();
    // an address that isn't a workspace's still names the warehouse, without a workspace ID
    assertThat(
            FabricWarehouseUtils.toOneLakeLocation(
                    new DatasetIdentifier("wh.raw.orders", "fabric-warehouse://example.com"))
                .get()
                .getWorkspaceId())
        .isNull();
  }

  @Test
  void testParseGuid() {
    assertThat(FabricWarehouseUtils.parseGuid(" d1f6473b-9f7a-4352-940b-a8d74461d3fc "))
        .contains(WORKSPACE);
    assertThat(FabricWarehouseUtils.parseGuid("wh_primary")).isEmpty();
    assertThat(FabricWarehouseUtils.parseGuid(null)).isEqualTo(Optional.empty());
    // UUID.fromString accepts these short forms, but they aren't Fabric IDs
    assertThat(FabricWarehouseUtils.parseGuid("1-1-1-1-1")).isEmpty();
    assertThat(FabricWarehouseUtils.parseGuid("d1f6473b-9f7a-4352-940b-a8d74461d3f")).isEmpty();
  }
}
