/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.utils;

import static org.assertj.core.api.Assertions.assertThat;

import io.openlineage.client.OpenLineage;
import java.net.URI;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

@SuppressWarnings("PMD.AvoidDuplicateLiterals")
class OneLakeLocationTest {
  private static final UUID WORKSPACE = UUID.fromString("d1f6473b-9f7a-4352-940b-a8d74461d3fc");
  private static final UUID LAKEHOUSE = UUID.fromString("c7a14054-60a7-4121-8fb7-252d16f687eb");
  private static final String NAMESPACE =
      "abfss://d1f6473b-9f7a-4352-940b-a8d74461d3fc@onelake.dfs.fabric.microsoft.com";
  private static final String GUID_PATH =
      NAMESPACE + "/c7a14054-60a7-4121-8fb7-252d16f687eb/Tables/sales/orders";

  @Test
  void testParseGuidForm() {
    OneLakeLocation location =
        OneLakeLocation.parse(NAMESPACE, "c7a14054-60a7-4121-8fb7-252d16f687eb/Tables/sales/orders")
            .get();

    assertThat(location.getWorkspaceId()).isEqualTo(WORKSPACE);
    assertThat(location.getItemId()).isEqualTo(LAKEHOUSE);
    assertThat(location.getWorkspaceName()).isNull();
    assertThat(location.getItemName()).isNull();
    assertThat(location.getItemType()).isNull();
    assertThat(location.getPath()).containsExactly("Tables", "sales", "orders");
    assertThat(location.guidLocation()).contains(GUID_PATH);
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "abfss://D1F6473B-9F7A-4352-940B-A8D74461D3FC@onelake.dfs.fabric.microsoft.com/"
            + "C7A14054-60A7-4121-8FB7-252D16F687EB/Tables/sales/orders/",
        "abfss://d1f6473b-9f7a-4352-940b-a8d74461d3fc@ONELAKE.BLOB.FABRIC.MICROSOFT.COM/"
            + "c7a14054-60a7-4121-8fb7-252d16f687eb/tables/sales/orders",
        "abfss://d1f6473b-9f7a-4352-940b-a8d74461d3fc@westus-onelake.dfs.fabric.microsoft.com/"
            + "c7a14054-60a7-4121-8fb7-252d16f687eb//Tables/sales/orders",
        "abfs://d1f6473b-9f7a-4352-940b-a8d74461d3fc@onelake.dfs.fabric.microsoft.com/"
            + "c7a14054-60a7-4121-8fb7-252d16f687eb/Tables/sales/orders"
      })
  void testCanonicalGuidLocation(String path) {
    assertThat(OneLakeLocation.parse(path).flatMap(OneLakeLocation::guidLocation))
        .contains(GUID_PATH);
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        // not OneLake
        "abfss://container@account.dfs.core.windows.net/c7a14054-60a7-4121-8fb7-252d16f687eb/Tables/t",
        "s3://d1f6473b-9f7a-4352-940b-a8d74461d3fc@onelake.dfs.fabric.microsoft.com/x/Tables/t",
        // no item
        "abfss://d1f6473b-9f7a-4352-940b-a8d74461d3fc@onelake.dfs.fabric.microsoft.com",
        // written with names
        "abfss://sales-analytics@onelake.dfs.fabric.microsoft.com/lh_sales.Lakehouse/Tables/t",
        // IDs and names mixed, which OneLake rejects
        "abfss://d1f6473b-9f7a-4352-940b-a8d74461d3fc@onelake.dfs.fabric.microsoft.com/"
            + "lh_sales.Lakehouse/Tables/t",
        "abfss://ws@onelake.dfs.fabric.microsoft.com/c7a14054-60a7-4121-8fb7-252d16f687eb/Tables/t",
        "Tables/t"
      })
  void testParseRejects(String path) {
    assertThat(OneLakeLocation.parse(path)).isEmpty();
  }

  @Test
  void testToFacet() {
    OpenLineage openLineage = new OpenLineage(URI.create("https://producer"));
    OpenLineage.FabricOneLakeDatasetFacet facet =
        OneLakeLocation.parse(GUID_PATH).get().toBuilder()
            .workspaceName("sales-analytics")
            .itemName("lh_sales")
            .itemType("Lakehouse")
            .tableId("6d3a2bc0-7f1e-4f4b-9a39-2c4e8f1d2b7a")
            .build()
            .toFacet(openLineage);

    assertThat(facet.getLocation()).isEqualTo(GUID_PATH);
    assertThat(facet.getWorkspaceId()).isEqualTo(WORKSPACE.toString());
    assertThat(facet.getWorkspaceName()).isEqualTo("sales-analytics");
    assertThat(facet.getItemId()).isEqualTo(LAKEHOUSE.toString());
    assertThat(facet.getItemName()).isEqualTo("lh_sales");
    assertThat(facet.getItemType()).isEqualTo("Lakehouse");
    assertThat(facet.getTableId()).isEqualTo("6d3a2bc0-7f1e-4f4b-9a39-2c4e8f1d2b7a");
  }

  @Test
  void testToFacetWithoutIds() {
    OpenLineage.FabricOneLakeDatasetFacet facet =
        OneLakeLocation.builder()
            .itemName("lh_sales")
            .build()
            .toFacet(new OpenLineage(URI.create("https://producer")));

    assertThat(facet.getItemName()).isEqualTo("lh_sales");
    assertThat(facet.getLocation()).isNull();
    assertThat(facet.getWorkspaceId()).isNull();
    assertThat(facet.getItemType()).isNull();
  }

  @Test
  void testToFacetLowercasesTableId() {
    OpenLineage.FabricOneLakeDatasetFacet facet =
        OneLakeLocation.parse(GUID_PATH).get().toBuilder()
            .tableId("6D3A2BC0-7F1E-4F4B-9A39-2C4E8F1D2B7A")
            .build()
            .toFacet(new OpenLineage(URI.create("https://producer")));

    assertThat(facet.getTableId()).isEqualTo("6d3a2bc0-7f1e-4f4b-9a39-2c4e8f1d2b7a");
  }

  @Test
  void testItemRootHasNoLocation() {
    // known workspace and item, but no table or file under the item
    OneLakeLocation location =
        OneLakeLocation.builder()
            .workspaceId(WORKSPACE)
            .itemId(LAKEHOUSE)
            .workspaceName("sales-analytics")
            .itemName("lh_sales")
            .itemType("Lakehouse")
            .build();

    assertThat(location.guidIdentifier()).isEmpty();
    OpenLineage.FabricOneLakeDatasetFacet facet =
        location.toFacet(new OpenLineage(URI.create("https://producer")));
    assertThat(facet.getWorkspaceId()).isEqualTo(WORKSPACE.toString());
    assertThat(facet.getItemId()).isEqualTo(LAKEHOUSE.toString());
    assertThat(facet.getLocation()).isNull();
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        NAMESPACE + "/c7a14054-60a7-4121-8fb7-252d16f687eb",
        NAMESPACE + "/c7a14054-60a7-4121-8fb7-252d16f687eb/"
      })
  void testParsedItemRootHasNoLocation(String path) {
    OneLakeLocation location = OneLakeLocation.parse(path).get();

    assertThat(location.getPath()).isEmpty();
    assertThat(location.guidIdentifier()).isEmpty();
    assertThat(location.toFacet(new OpenLineage(URI.create("https://producer"))).getLocation())
        .isNull();
  }

  @Test
  void testGuid() {
    assertThat(OneLakeLocation.guid(" C7A14054-60A7-4121-8FB7-252D16F687EB ")).contains(LAKEHOUSE);
    assertThat(OneLakeLocation.guid("lh_sales")).isEmpty();
    assertThat(OneLakeLocation.guid(null)).isEqualTo(Optional.empty());
  }
}
