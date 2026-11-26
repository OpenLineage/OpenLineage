/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.utils.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.client.utils.DatasetIdentifier.Symlink;
import io.openlineage.client.utils.DatasetIdentifier.SymlinkType;
import java.util.Properties;
import org.junit.jupiter.api.Test;

@SuppressWarnings("PMD.AvoidDuplicateLiterals")
class JdbcDatasetUtilsTestForFabricWarehouse {
  private static final String HOST = "ck32i2hmt2qeziwtnklnft2ptq-hnd7nul2t5jehfalvdluiyot7q";
  private static final String NAMESPACE =
      "fabric-warehouse://" + HOST + ".datawarehouse.fabric.microsoft.com";

  @Test
  void testConnectorUrlWithZoneLabel() {
    // the URL the Fabric connector builds when it looks up the address itself
    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:sqlserver://;servername="
                    + HOST
                    + ".z8b.datawarehouse.fabric.microsoft.com;encrypt=true;databaseName=wh_primary",
                "raw.orders",
                new Properties()))
        .hasFieldOrPropertyWithValue("namespace", NAMESPACE)
        .hasFieldOrPropertyWithValue("name", "wh_primary.raw.orders");
  }

  @Test
  void testConnectorUrlWithoutZoneLabel() {
    // the URL the Fabric connector builds from an address the user set in
    // spark.datawarehouse.<wh>.sqlendpoint
    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:sqlserver://;servername="
                    + HOST
                    + ".datawarehouse.fabric.microsoft.com;databaseName=wh_primary",
                "raw.orders",
                new Properties()))
        .hasFieldOrPropertyWithValue("namespace", NAMESPACE)
        .hasFieldOrPropertyWithValue("name", "wh_primary.raw.orders");
  }

  @Test
  void testFullyQualifiedName() {
    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:sqlserver://;servername="
                    + HOST
                    + ".z8b.datawarehouse.fabric.microsoft.com;databaseName=wh_primary",
                "wh_primary.raw.orders",
                new Properties()))
        .hasFieldOrPropertyWithValue("namespace", NAMESPACE)
        .hasFieldOrPropertyWithValue("name", "wh_primary.raw.orders");
  }

  @Test
  void testDefaultPortIsDropped() {
    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:sqlserver://" + HOST + ".z8b.datawarehouse.fabric.microsoft.com:1433",
                "wh_primary.raw.orders",
                new Properties()))
        .hasFieldOrPropertyWithValue("namespace", NAMESPACE);

    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:sqlserver://;servername="
                    + HOST
                    + ".datawarehouse.fabric.microsoft.com,1433;databaseName=wh_primary",
                "raw.orders",
                new Properties()))
        .hasFieldOrPropertyWithValue("namespace", NAMESPACE);
  }

  @Test
  void testNonDefaultPortIsKept() {
    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:sqlserver://" + HOST + ".datawarehouse.fabric.microsoft.com:1444",
                "wh_primary.raw.orders",
                new Properties()))
        .hasFieldOrPropertyWithValue("namespace", NAMESPACE + ":1444");
  }

  @Test
  void testAzureSqlStaysSqlServer() {
    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:sqlserver://myserver.database.windows.net:1433;databaseName=sales",
                "dbo.orders",
                new Properties()))
        .hasFieldOrPropertyWithValue("namespace", "sqlserver://myserver.database.windows.net:1433")
        .hasFieldOrPropertyWithValue("name", "sales.dbo.orders");
  }

  @Test
  void testGuestHostStaysSqlServer() {
    String guest =
        "45jm47djf4je5afhvapdos5fae-gakpgz3u6twe7nemz77to4q2ti-jh0bi.zcf.datawarehouse.fabric.microsoft.com";
    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:sqlserver://;servername=" + guest + ";databaseName=wh",
                "raw.orders",
                new Properties()))
        .hasFieldOrPropertyWithValue("namespace", "sqlserver://" + guest);
  }

  private static final String CONNECTOR_URL =
      "jdbc:sqlserver://;servername="
          + HOST
          + ".z8b.datawarehouse.fabric.microsoft.com;databaseName=wh_primary";
  private static final String ONELAKE =
      "abfss://d1f6473b-9f7a-4352-940b-a8d74461d3fc@onelake.dfs.fabric.microsoft.com";
  private static final String WAREHOUSE_ID = "d75873bd-716b-4378-a37e-864c76fc9b3b";

  @Test
  void testConnectorOptionsAddTheOneLakeLocation() {
    // the Fabric connector passes the user's options through to the JDBC read
    Properties properties = new Properties();
    properties.setProperty("datawarehouseid", WAREHOUSE_ID);

    DatasetIdentifier identifier =
        JdbcDatasetUtils.getDatasetIdentifier(CONNECTOR_URL, "raw.orders", properties);

    assertThat(identifier.getSymlinks())
        .containsExactly(
            new Symlink(WAREHOUSE_ID + "/Tables/raw/orders", ONELAKE, SymlinkType.LOCATION));
  }

  @Test
  void testDatabaseNameOptionNamesTheWarehouseTheIdIsFor() {
    Properties properties = new Properties();
    properties.setProperty("DatawarehouseId", WAREHOUSE_ID);
    properties.setProperty("database_name", "wh_other");

    // the query runs in wh_other, so the ID is wh_other's, not that of the URL's database
    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(CONNECTOR_URL, "raw.orders", properties)
                .getSymlinks())
        .isEmpty();
    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(CONNECTOR_URL, "wh_other.raw.orders", properties)
                .getSymlinks())
        .containsExactly(
            new Symlink(WAREHOUSE_ID + "/Tables/raw/orders", ONELAKE, SymlinkType.LOCATION));
  }

  @Test
  void testNoLocationWithoutTheWarehouseId() {
    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(CONNECTOR_URL, "raw.orders", new Properties())
                .getSymlinks())
        .isEmpty();
  }

  @Test
  void testBlankOptionsAreIgnored() {
    // a blank database_name falls back to the URL's database, and a blank WorkspaceId to the
    // address's workspace
    Properties properties = new Properties();
    properties.setProperty("DatawarehouseId", WAREHOUSE_ID);
    properties.setProperty("database_name", " ");
    properties.setProperty("WorkspaceId", "");

    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(CONNECTOR_URL, "raw.orders", properties)
                .getSymlinks())
        .containsExactly(
            new Symlink(WAREHOUSE_ID + "/Tables/raw/orders", ONELAKE, SymlinkType.LOCATION));
  }
}
