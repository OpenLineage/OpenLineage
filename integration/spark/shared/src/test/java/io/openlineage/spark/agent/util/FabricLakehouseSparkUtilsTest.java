/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.util;

import static org.assertj.core.api.Assertions.assertThat;

import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.client.utils.DatasetIdentifier.Symlink;
import io.openlineage.client.utils.DatasetIdentifier.SymlinkType;
import io.openlineage.client.utils.FabricWarehouseUtils;
import io.openlineage.client.utils.OneLakeLocation;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.Test;

// The IDs and names below are made up
@SuppressWarnings("PMD.AvoidDuplicateLiterals")
class FabricLakehouseSparkUtilsTest {
  private static final String TENANT = "f3c1e2a4-7b8d-4e6f-9a0b-1c2d3e4f5a6b";
  private static final String WS = "d1f6473b-9f7a-4352-940b-a8d74461d3fc";
  private static final String OTHER_WS = "45ff911e-5e75-428a-a618-52958fcc7604";
  private static final String LH = "c7a14054-60a7-4121-8fb7-252d16f687eb";
  private static final String OTHER_LH = "d8a75818-4429-41c0-bc80-81da494ec02f";
  private static final String ONELAKE = "abfss://" + WS + "@onelake.dfs.fabric.microsoft.com";
  private static final String BY_NAME = "abfss://sales-analytics@onelake.dfs.fabric.microsoft.com";
  private static final String LAKEHOUSE_NS = "fabric-lakehouse://" + WS;

  private final Map<String, String> conf = new HashMap<>();

  private FabricLakehouseSparkUtils.Session session() {
    return FabricLakehouseSparkUtils.Session.of(key -> Optional.ofNullable(conf.get(key))).get();
  }

  private DatasetIdentifier resolve(String name, String namespace) {
    return FabricLakehouseSparkUtils.resolve(new DatasetIdentifier(name, namespace), session());
  }

  private void fabricSession() {
    conf.put("trident.tenant.id", TENANT);
    conf.put("trident.workspace.id", WS);
    conf.put("trident.workspace.name", "sales-analytics");
    conf.put("trident.lakehouse.id", LH);
    conf.put("trident.lakehouse.name", "lh_sales");
  }

  @Test
  void testNoSessionOutsideFabric() {
    assertThat(FabricLakehouseSparkUtils.Session.of(key -> Optional.empty())).isEmpty();
  }

  @Test
  void testDefaultLakehouseTable() {
    fabricSession();

    DatasetIdentifier named = resolve(LH + "/Tables/sales/orders", ONELAKE);

    assertThat(named.getNamespace()).isEqualTo(LAKEHOUSE_NS);
    assertThat(named.getName()).isEqualTo("lh_sales.sales.orders");
    assertThat(named.getSymlinks())
        .containsExactly(new Symlink(LH + "/Tables/sales/orders", ONELAKE, SymlinkType.LOCATION));
  }

  @Test
  void testDefaultLakehouseWithoutSchemas() {
    fabricSession();

    assertThat(resolve(LH + "/Tables/orders", ONELAKE).getName()).isEqualTo("lh_sales.dbo.orders");
  }

  @Test
  void testKeepsExistingSymlinks() {
    fabricSession();
    DatasetIdentifier identifier =
        new DatasetIdentifier(LH + "/Tables/sales/orders", ONELAKE)
            .withSymlink("db.orders", "file:/warehouse", SymlinkType.TABLE);

    assertThat(FabricLakehouseSparkUtils.resolve(identifier, session()).getSymlinks())
        .contains(new Symlink("db.orders", "file:/warehouse", SymlinkType.TABLE));
  }

  @Test
  void testPathsThatDontShowALakehouseKeepTheirName() {
    fabricSession();

    // another item: the path doesn't say it's a lakehouse
    assertUnchanged(OTHER_LH + "/Tables/sales/orders", ONELAKE);
    // another workspace
    assertUnchanged(
        LH + "/Tables/sales/orders", "abfss://" + OTHER_WS + "@onelake.dfs.fabric.microsoft.com");
    // files, not a table
    assertUnchanged(LH + "/Files/raw/a.csv", ONELAKE);
    assertUnchanged(LH + "/Tables/a/b/c", ONELAKE);
    // not OneLake
    assertUnchanged("warehouse/orders", "abfss://container@account.dfs.core.windows.net");
  }

  @Test
  void testWarehousePathWithNamesInTheSessionsWorkspace() {
    fabricSession();
    String warehouseNs =
        "fabric-warehouse://"
            + FabricWarehouseUtils.endpointFor(UUID.fromString(TENANT), UUID.fromString(WS));

    DatasetIdentifier warehouse = resolve("wh_sales.Warehouse/Tables/raw/orders", BY_NAME);
    assertThat(warehouse.getNamespace()).isEqualTo(warehouseNs);
    assertThat(warehouse.getName()).isEqualTo("wh_sales.raw.orders");
    // its ID isn't known, so there's no location written with IDs
    assertThat(warehouse.getSymlinks()).isEmpty();

    DatasetIdentifier datawarehouse = resolve("wh_sales.datawarehouse/tables/raw/orders/", BY_NAME);
    assertThat(datawarehouse.getNamespace()).isEqualTo(warehouseNs);
    assertThat(datawarehouse.getName()).isEqualTo("wh_sales.raw.orders");
  }

  @Test
  void testWarehousePathsWithNamesThatKeepTheirName() {
    fabricSession();

    // warehouses always have schemas
    assertUnchanged("wh_sales.Warehouse/Tables/orders", BY_NAME);
    assertUnchanged("wh_sales.Warehouse/Files/raw/a.csv", BY_NAME);
    assertUnchanged(".Warehouse/Tables/raw/orders", BY_NAME);
    // another workspace
    assertUnchanged(
        "wh_sales.Warehouse/Tables/raw/orders",
        "abfss://other-ws@onelake.dfs.fabric.microsoft.com");
    // without the tenant, the SQL address can't be built
    conf.remove("trident.tenant.id");
    assertUnchanged("wh_sales.Warehouse/Tables/raw/orders", BY_NAME);
  }

  @Test
  void testDefaultLakehouseWithoutItsName() {
    conf.put("trident.workspace.id", WS);
    conf.put("trident.lakehouse.id", LH);

    assertUnchanged(LH + "/Tables/sales/orders", ONELAKE);
  }

  @Test
  void testPathWithNamesInTheSessionsWorkspace() {
    fabricSession();

    DatasetIdentifier other = resolve("lh_other.Lakehouse/Tables/orders", BY_NAME);
    assertThat(other.getNamespace()).isEqualTo(LAKEHOUSE_NS);
    assertThat(other.getName()).isEqualTo("lh_other.dbo.orders");
    // its ID isn't known, so there's no location written with IDs
    assertThat(other.getSymlinks()).isEmpty();

    // the default lakehouse's ID is known
    DatasetIdentifier main = resolve("lh_sales.lakehouse/tables/sales/orders/", BY_NAME);
    assertThat(main.getName()).isEqualTo("lh_sales.sales.orders");
    assertThat(main.getSymlinks())
        .containsExactly(new Symlink(LH + "/Tables/sales/orders", ONELAKE, SymlinkType.LOCATION));
  }

  @Test
  void testPathWithNamesDecodesTheWorkspace() {
    fabricSession();
    conf.put("trident.workspace.name", "Sales Team");

    assertThat(
            resolve(
                    "lh_other.Lakehouse/Tables/orders",
                    "abfss://Sales%20Team@onelake.dfs.fabric.microsoft.com")
                .getName())
        .isEqualTo("lh_other.dbo.orders");
  }

  @Test
  void testPathWithNamesInAnotherWorkspace() {
    fabricSession();

    assertUnchanged(
        "lh_other.Lakehouse/Tables/orders", "abfss://other-ws@onelake.dfs.fabric.microsoft.com");
    // without the session's workspace name, its workspace isn't known
    conf.remove("trident.workspace.name");
    assertUnchanged("lh_other.Lakehouse/Tables/orders", BY_NAME);
  }

  @Test
  void testToOneLakeLocation() {
    fabricSession();
    DatasetIdentifier named = resolve(LH + "/Tables/sales/orders", ONELAKE);

    OneLakeLocation location = FabricLakehouseSparkUtils.toOneLakeLocation(named).get();

    assertThat(location.getWorkspaceId()).isEqualTo(UUID.fromString(WS));
    assertThat(location.getItemName()).isEqualTo("lh_sales");
    assertThat(location.getItemType()).isEqualTo("Lakehouse");
    assertThat(location.getItemId()).isEqualTo(UUID.fromString(LH));
    assertThat(location.guidLocation()).contains(ONELAKE + "/" + LH + "/Tables/sales/orders");
  }

  @Test
  void testToOneLakeLocationWithoutTheLakehousesId() {
    OneLakeLocation location =
        FabricLakehouseSparkUtils.toOneLakeLocation(
                new DatasetIdentifier("lh_other.dbo.orders", LAKEHOUSE_NS))
            .get();

    assertThat(location.getItemName()).isEqualTo("lh_other");
    assertThat(location.getItemId()).isNull();
    assertThat(location.guidLocation()).isEmpty();
    assertThat(
            FabricLakehouseSparkUtils.toOneLakeLocation(
                new DatasetIdentifier("orders", LAKEHOUSE_NS)))
        .isEmpty();
  }

  private void assertUnchanged(String name, String namespace) {
    DatasetIdentifier identifier = new DatasetIdentifier(name, namespace);
    assertThat(FabricLakehouseSparkUtils.resolve(identifier, session())).isSameAs(identifier);
  }
}
