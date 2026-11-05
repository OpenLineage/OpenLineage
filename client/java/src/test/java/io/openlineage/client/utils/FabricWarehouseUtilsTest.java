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
  private static final UUID TENANT = UUID.fromString("cc0b82f3-7c2e-400b-aec3-40a3d720505b");
  private static final UUID WORKSPACE = UUID.fromString("8b4cb800-a044-43ce-8f76-3356f709b468");
  private static final UUID WORKSPACE_2 = UUID.fromString("bcc59eac-e270-40a8-9deb-f8f5994596a5");
  private static final UUID WAREHOUSE = UUID.fromString("0fd9a0c2-ee93-46af-aaea-a45c8649ed9d");
  private static final String ENDPOINT =
      "6obaxtbopqfublwdicr5oicqlm-ac4ezc2eudhehd3wgnlpocnuna.datawarehouse.fabric.microsoft.com";
  private static final String ENDPOINT_2 =
      "6obaxtbopqfublwdicr5oicqlm-vspmlpdq4kuebhpl7d2zsrmwuu.datawarehouse.fabric.microsoft.com";

  @Test
  void testEndpointFor() {
    assertThat(FabricWarehouseUtils.endpointFor(TENANT, WORKSPACE)).isEqualTo(ENDPOINT);
  }

  @Test
  void testEncodeGuid() {
    assertThat(FabricWarehouseUtils.encodeGuid(WORKSPACE_2))
        .isEqualTo("vspmlpdq4kuebhpl7d2zsrmwuu");
  }

  @Test
  void testDecodeGuidRoundTrip() {
    assertThat(FabricWarehouseUtils.decodeGuid("vspmlpdq4kuebhpl7d2zsrmwuu")).contains(WORKSPACE_2);
    assertThat(FabricWarehouseUtils.decodeGuid(FabricWarehouseUtils.encodeGuid(TENANT)))
        .contains(TENANT);
  }

  @Test
  void testDecodeGuidRejectsMalformedLabels() {
    assertThat(FabricWarehouseUtils.decodeGuid("vspmlpdq4kuebhpl7d2zsrmwu")).isEmpty();
    assertThat(FabricWarehouseUtils.decodeGuid("vspmlpdq4kuebhpl7d2zsrmwu1")).isEmpty();
    // the last character leaves extra bits that aren't zero, so it can't come from an ID
    assertThat(FabricWarehouseUtils.decodeGuid("vspmlpdq4kuebhpl7d2zsrmwuv")).isEmpty();
  }

  @ParameterizedTest
  @CsvSource(
      delimiter = '|',
      value = {
        "6obaxtbopqfublwdicr5oicqlm-ac4ezc2eudhehd3wgnlpocnuna.z8b.datawarehouse.fabric.microsoft.com,1433"
            + " | "
            + ENDPOINT,
        "6obaxtbopqfublwdicr5oicqlm-vspmlpdq4kuebhpl7d2zsrmwuu.zbc.datawarehouse.fabric.microsoft.com:1433"
            + " | "
            + ENDPOINT_2,
        ENDPOINT + ":1444 | " + ENDPOINT + ":1444",
        ENDPOINT + ",1444 | " + ENDPOINT + ":1444",
        "6OBAXTBOPQFUBLWDICR5OICQLM-AC4EZC2EUDHEHD3WGNLPOCNUNA.DATAWAREHOUSE.FABRIC.MICROSOFT.COM | "
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
        "6obaxtbopqfublwdicr5oicqlm-ac4ezc2eudhehd3wgnlpocnuna.z8b.datawarehouse.fabric.microsoft.com:1433 | true",
        // an address for reaching a workspace in another organization has an extra part
        "qvrmbuxie7we7glrekxgy6npqu-6xgyei3x2xiejip4iime6knh5m-jh0bi.zcf.datawarehouse.fabric.microsoft.com | false",
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
                "6obaxtbopqfublwdicr5oicqlm-vspmlpdq4kuebhpl7d2zsrmwuu.zbc.datawarehouse.fabric.microsoft.com"))
        .contains(WORKSPACE_2);
    assertThat(FabricWarehouseUtils.workspaceIdOf("myserver.database.windows.net")).isEmpty();
  }

  @Test
  void testOneLakeLocation() {
    UUID warehouse = UUID.fromString("0fd9a0c2-ee93-46af-aaea-a45c8649ed9d");

    DatasetIdentifier location =
        FabricWarehouseUtils.oneLakeLocation(WORKSPACE, warehouse, "raw", "orders");

    assertThat(location.getNamespace())
        .isEqualTo("abfss://8b4cb800-a044-43ce-8f76-3356f709b468@onelake.dfs.fabric.microsoft.com");
    assertThat(location.getName())
        .isEqualTo("0fd9a0c2-ee93-46af-aaea-a45c8649ed9d/Tables/raw/orders");
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
    assertThat(FabricWarehouseUtils.parseGuid(" 8b4cb800-a044-43ce-8f76-3356f709b468 "))
        .contains(WORKSPACE);
    assertThat(FabricWarehouseUtils.parseGuid("wh_primary")).isEmpty();
    assertThat(FabricWarehouseUtils.parseGuid(null)).isEqualTo(Optional.empty());
  }
}
