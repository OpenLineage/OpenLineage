/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.utils.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Properties;
import org.junit.jupiter.api.Test;

@SuppressWarnings("PMD.AvoidDuplicateLiterals")
class JdbcDatasetUtilsTestForFabricWarehouse {
  private static final String HOST = "6obaxtbopqfublwdicr5oicqlm-ac4ezc2eudhehd3wgnlpocnuna";
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
        "qvrmbuxie7we7glrekxgy6npqu-6xgyei3x2xiejip4iime6knh5m-jh0bi.zcf.datawarehouse.fabric.microsoft.com";
    assertThat(
            JdbcDatasetUtils.getDatasetIdentifier(
                "jdbc:sqlserver://;servername=" + guest + ";databaseName=wh",
                "raw.orders",
                new Properties()))
        .hasFieldOrPropertyWithValue("namespace", "sqlserver://" + guest);
  }
}
