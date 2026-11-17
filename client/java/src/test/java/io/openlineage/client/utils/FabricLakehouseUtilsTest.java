/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.utils;

import static org.assertj.core.api.Assertions.assertThat;

import io.openlineage.client.utils.DatasetIdentifier.Symlink;
import io.openlineage.client.utils.DatasetIdentifier.SymlinkType;
import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;
import org.junit.jupiter.api.Test;

// The IDs and names below are made up
class FabricLakehouseUtilsTest {
  private static final UUID WS = UUID.fromString("d1f6473b-9f7a-4352-940b-a8d74461d3fc");
  private static final UUID LH = UUID.fromString("c7a14054-60a7-4121-8fb7-252d16f687eb");
  private static final String ONELAKE = "abfss://" + WS + "@onelake.dfs.fabric.microsoft.com";
  private static final String NAMESPACE = "fabric-lakehouse://" + WS;

  private static OneLakeLocation location(UUID itemId, String... path) {
    return OneLakeLocation.builder()
        .workspaceId(WS)
        .itemId(itemId)
        .path(Arrays.asList(path))
        .build();
  }

  @Test
  void testTableInALakehouseWithSchemas() {
    DatasetIdentifier table =
        FabricLakehouseUtils.tableAt("lh_sales", location(LH, "Tables", "sales", "orders"), null)
            .get();

    assertThat(table.getNamespace()).isEqualTo(NAMESPACE);
    assertThat(table.getName()).isEqualTo("lh_sales.sales.orders");
    assertThat(table.getSymlinks())
        .containsExactly(new Symlink(LH + "/Tables/sales/orders", ONELAKE, SymlinkType.LOCATION));
  }

  @Test
  void testTableInALakehouseWithoutSchemas() {
    DatasetIdentifier table =
        FabricLakehouseUtils.tableAt(
                "lh_sales",
                location(null, "Tables", "orders"),
                Collections.singletonList(new Symlink("db.orders", "file:/w", SymlinkType.TABLE)))
            .get();

    assertThat(table.getName()).isEqualTo("lh_sales.dbo.orders");
    // the lakehouse's ID isn't known, so there's no location written with IDs
    assertThat(table.getSymlinks())
        .containsExactly(new Symlink("db.orders", "file:/w", SymlinkType.TABLE));
  }

  @Test
  void testNotATable() {
    assertThat(FabricLakehouseUtils.tableAt("lh", location(LH, "Files", "raw", "a.csv"), null))
        .isEmpty();
    assertThat(FabricLakehouseUtils.tableAt("lh", location(LH, "Tables", "a", "b", "c"), null))
        .isEmpty();
    assertThat(FabricLakehouseUtils.tableAt("lh", location(LH, "Tables"), null)).isEmpty();
  }

  @Test
  void testToOneLakeLocationRoundTrips() {
    DatasetIdentifier table =
        FabricLakehouseUtils.tableAt("lh_sales", location(LH, "Tables", "sales", "orders"), null)
            .get();

    OneLakeLocation location = FabricLakehouseUtils.toOneLakeLocation(table).get();

    assertThat(location.getWorkspaceId()).isEqualTo(WS);
    assertThat(location.getItemId()).isEqualTo(LH);
    assertThat(location.getItemName()).isEqualTo("lh_sales");
    assertThat(location.getItemType()).isEqualTo("Lakehouse");
    assertThat(location.guidLocation()).contains(ONELAKE + "/" + LH + "/Tables/sales/orders");
  }

  @Test
  void testToOneLakeLocationOfOtherDatasets() {
    assertThat(FabricLakehouseUtils.toOneLakeLocation(new DatasetIdentifier("a.b.c", "file:/x")))
        .isEmpty();
    assertThat(FabricLakehouseUtils.toOneLakeLocation(new DatasetIdentifier("orders", NAMESPACE)))
        .isEmpty();
  }
}
