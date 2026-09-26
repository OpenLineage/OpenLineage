/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark40.agent.lifecycle.plan.catalog;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.spark.sql.connector.catalog.Identifier;
import org.junit.jupiter.api.Test;

class UnityCatalogHandlerNamingTest {

  @Test
  void namespaceKeepsHostAndPortAndDropsScheme() {
    assertThat(UnityCatalogHandler.unityCatalogNamespace("https://my-uc.example.com:443"))
        .isEqualTo("unitycatalog://my-uc.example.com:443");
  }

  @Test
  void namespaceWithoutPort() {
    assertThat(
            UnityCatalogHandler.unityCatalogNamespace(
                "https://adb-123456789.0.azuredatabricks.net"))
        .isEqualTo("unitycatalog://adb-123456789.0.azuredatabricks.net");
  }

  @Test
  void namespaceDropsPath() {
    assertThat(UnityCatalogHandler.unityCatalogNamespace("https://my-uc.example.com/api/2.1/unity"))
        .isEqualTo("unitycatalog://my-uc.example.com");
  }

  @Test
  void namespaceIsStableAcrossProtocol() {
    // http and https with the same authority must produce the same namespace.
    assertThat(UnityCatalogHandler.unityCatalogNamespace("http://localhost:8080"))
        .isEqualTo("unitycatalog://localhost:8080")
        .isEqualTo(UnityCatalogHandler.unityCatalogNamespace("https://localhost:8080"));
  }

  @Test
  void nameIsCatalogSchemaTable() {
    assertThat(
            UnityCatalogHandler.unityCatalogName(
                "main", Identifier.of(new String[] {"sales"}, "orders")))
        .isEqualTo("main.sales.orders");
  }
}
