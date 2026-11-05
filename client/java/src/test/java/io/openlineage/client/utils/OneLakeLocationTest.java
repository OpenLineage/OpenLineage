/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.utils;

import static org.assertj.core.api.Assertions.assertThat;

import io.openlineage.client.OpenLineage;
import java.net.URI;
import java.util.Arrays;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

// The IDs and names below are those of a real Fabric test workspace
@SuppressWarnings("PMD.AvoidDuplicateLiterals")
class OneLakeLocationTest {
  private static final UUID WORKSPACE = UUID.fromString("8b4cb800-a044-43ce-8f76-3356f709b468");
  private static final UUID LAKEHOUSE = UUID.fromString("05bac213-0db7-4b56-ad4c-ac8617938d3f");
  private static final String NAMESPACE =
      "abfss://8b4cb800-a044-43ce-8f76-3356f709b468@onelake.dfs.fabric.microsoft.com";
  private static final String GUID_PATH =
      NAMESPACE + "/05bac213-0db7-4b56-ad4c-ac8617938d3f/Tables/sales/orders";

  @Test
  void testParseGuidForm() {
    OneLakeLocation location =
        OneLakeLocation.parse(NAMESPACE, "05bac213-0db7-4b56-ad4c-ac8617938d3f/Tables/sales/orders")
            .get();

    assertThat(location.getWorkspaceId()).isEqualTo(WORKSPACE);
    assertThat(location.getItemId()).isEqualTo(LAKEHOUSE);
    assertThat(location.getWorkspaceName()).isNull();
    assertThat(location.getItemName()).isNull();
    assertThat(location.getItemType()).isNull();
    assertThat(location.getPath()).containsExactly("Tables", "sales", "orders");
    assertThat(location.tableParts()).contains(Arrays.asList("sales", "orders"));
    assertThat(location.guidLocation()).contains(GUID_PATH);
  }

  @Test
  void testParseNameForm() {
    OneLakeLocation location =
        OneLakeLocation.parse(
                "abfss://do-test-env-fabric@onelake.dfs.fabric.microsoft.com/"
                    + "lh_shopist_legacy.Lakehouse/Tables/orders_daily")
            .get();

    assertThat(location.getWorkspaceName()).isEqualTo("do-test-env-fabric");
    assertThat(location.getItemName()).isEqualTo("lh_shopist_legacy");
    assertThat(location.getItemType()).isEqualTo(OneLakeLocation.LAKEHOUSE);
    assertThat(location.getWorkspaceId()).isNull();
    assertThat(location.tableParts()).contains(Arrays.asList("orders_daily"));
    assertThat(location.guidLocation()).isEmpty();
    assertThat(location.canonicalIdentifier())
        .contains(
            new DatasetIdentifier(
                "lh_shopist_legacy.Lakehouse/Tables/orders_daily",
                "abfss://do-test-env-fabric@onelake.dfs.fabric.microsoft.com"));
  }

  @Test
  void testSparkWarehouseSuffixIsCanonicalized() {
    // Spark reports warehouses as .Datawarehouse; OneLake's APIs use .Warehouse
    OneLakeLocation location =
        OneLakeLocation.parse(
                "abfss://do-test-env-fabric@onelake.dfs.fabric.microsoft.com/"
                    + "wh_primary.Datawarehouse/Tables/raw/orders")
            .get();

    assertThat(location.getItemType()).isEqualTo(OneLakeLocation.WAREHOUSE);
    assertThat(location.canonicalIdentifier().get().getName())
        .isEqualTo("wh_primary.Warehouse/Tables/raw/orders");
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "abfss://8B4CB800-A044-43CE-8F76-3356F709B468@onelake.dfs.fabric.microsoft.com/"
            + "05BAC213-0DB7-4B56-AD4C-AC8617938D3F/Tables/sales/orders/",
        "abfss://8b4cb800-a044-43ce-8f76-3356f709b468@ONELAKE.BLOB.FABRIC.MICROSOFT.COM/"
            + "05bac213-0db7-4b56-ad4c-ac8617938d3f/tables/sales/orders",
        "abfss://8b4cb800-a044-43ce-8f76-3356f709b468@westus-onelake.dfs.fabric.microsoft.com/"
            + "05bac213-0db7-4b56-ad4c-ac8617938d3f//Tables/sales/orders",
        "abfs://8b4cb800-a044-43ce-8f76-3356f709b468@onelake.dfs.fabric.microsoft.com/"
            + "05bac213-0db7-4b56-ad4c-ac8617938d3f/Tables/sales/orders"
      })
  void testCanonicalGuidLocation(String path) {
    assertThat(OneLakeLocation.parse(path).flatMap(OneLakeLocation::guidLocation))
        .contains(GUID_PATH);
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        // not OneLake
        "abfss://container@account.dfs.core.windows.net/05bac213-0db7-4b56-ad4c-ac8617938d3f/Tables/t",
        "s3://8b4cb800-a044-43ce-8f76-3356f709b468@onelake.dfs.fabric.microsoft.com/x/Tables/t",
        // no item
        "abfss://8b4cb800-a044-43ce-8f76-3356f709b468@onelake.dfs.fabric.microsoft.com",
        // a bare item name, which OneLake rejects
        "abfss://ws@onelake.dfs.fabric.microsoft.com/lh_shopist/Tables/t",
        // IDs and names mixed, which OneLake rejects
        "abfss://8b4cb800-a044-43ce-8f76-3356f709b468@onelake.dfs.fabric.microsoft.com/"
            + "lh_shopist.Lakehouse/Tables/t",
        "abfss://ws@onelake.dfs.fabric.microsoft.com/05bac213-0db7-4b56-ad4c-ac8617938d3f/Tables/t",
        "Tables/t"
      })
  void testParseRejects(String path) {
    assertThat(OneLakeLocation.parse(path)).isEmpty();
  }

  @Test
  void testTablePartsOnlyForTables() {
    assertThat(OneLakeLocation.parse(NAMESPACE + "/" + LAKEHOUSE + "/Files/raw/a.csv").get())
        .satisfies(l -> assertThat(l.tableParts()).isEmpty());
    assertThat(OneLakeLocation.parse(NAMESPACE + "/" + LAKEHOUSE + "/Tables").get())
        .satisfies(l -> assertThat(l.tableParts()).isEmpty());
    assertThat(OneLakeLocation.parse(NAMESPACE + "/" + LAKEHOUSE + "/Tables/a/b/c").get())
        .satisfies(l -> assertThat(l.tableParts()).isEmpty());
  }

  @Test
  void testToFacet() {
    OpenLineage openLineage = new OpenLineage(URI.create("https://producer"));
    OpenLineage.OneLakeDatasetFacet facet =
        OneLakeLocation.parse(GUID_PATH).get().toBuilder()
            .workspaceName("do-test-env-fabric")
            .itemName("lh_shopist")
            .itemType(OneLakeLocation.LAKEHOUSE)
            .tableId("6d3a2bc0-7f1e-4f4b-9a39-2c4e8f1d2b7a")
            .build()
            .toFacet(openLineage);

    assertThat(facet.getLocation()).isEqualTo(GUID_PATH);
    assertThat(facet.getWorkspaceId()).isEqualTo(WORKSPACE.toString());
    assertThat(facet.getWorkspaceName()).isEqualTo("do-test-env-fabric");
    assertThat(facet.getItemId()).isEqualTo(LAKEHOUSE.toString());
    assertThat(facet.getItemName()).isEqualTo("lh_shopist");
    assertThat(facet.getItemType()).isEqualTo("Lakehouse");
    assertThat(facet.getTableId()).isEqualTo("6d3a2bc0-7f1e-4f4b-9a39-2c4e8f1d2b7a");
  }

  @Test
  void testToFacetWithoutIds() {
    OpenLineage.OneLakeDatasetFacet facet =
        OneLakeLocation.builder()
            .itemName("lh_shopist")
            .build()
            .toFacet(new OpenLineage(URI.create("https://producer")));

    assertThat(facet.getItemName()).isEqualTo("lh_shopist");
    assertThat(facet.getLocation()).isNull();
    assertThat(facet.getWorkspaceId()).isNull();
    assertThat(facet.getItemType()).isNull();
  }

  @Test
  void testGuid() {
    assertThat(OneLakeLocation.guid(" 05BAC213-0DB7-4B56-AD4C-AC8617938D3F ")).contains(LAKEHOUSE);
    assertThat(OneLakeLocation.guid("lh_shopist")).isEmpty();
    assertThat(OneLakeLocation.guid(null)).isEqualTo(Optional.empty());
  }
}
