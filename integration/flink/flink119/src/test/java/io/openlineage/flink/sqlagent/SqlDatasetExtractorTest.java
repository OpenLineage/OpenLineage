/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.sqlagent;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Arrays;
import org.junit.jupiter.api.Test;

class SqlDatasetExtractorTest {
  @Test
  void mapsFlinkCatalogAliasToGravitinoDatasetIdentity() {
    SqlDatasetExtractor extractor =
        new SqlDatasetExtractor("iceberg_catalog", "gv_iceberg", "metalake", "{}");

    assertEquals(
        Arrays.asList("metalake", "iceberg_catalog.flink_db.flink_sql_target"),
        extractor.identity("gv_iceberg", "flink_db", "flink_sql_target"));
  }

  @Test
  void supportsMultipleCatalogMappingsAndPreservesUnknownCatalogs() {
    SqlDatasetExtractor extractor =
        new SqlDatasetExtractor(
            "iceberg_catalog", "gv_iceberg", "metalake", "{\"gv_hive\":\"hive_catalog\"}");

    assertEquals(
        Arrays.asList("metalake", "hive_catalog.sales.orders"),
        extractor.identity("gv_hive", "sales", "orders"));
    assertEquals(
        Arrays.asList("metalake", "other.sales.orders"),
        extractor.identity("other", "sales", "orders"));
  }

  @Test
  void retainsLegacyIdentityWithoutMetalake() {
    SqlDatasetExtractor extractor =
        new SqlDatasetExtractor("iceberg_catalog", "gv_iceberg", "", "{}");

    assertEquals(
        Arrays.asList("iceberg_catalog", "flink_db.flink_sql_target"),
        extractor.identity("gv_iceberg", "flink_db", "flink_sql_target"));
  }

  @Test
  void rejectsInvalidCatalogMapping() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new SqlDatasetExtractor("", "", "metalake", "{\"gv_hive\":12}"));
  }

  @Test
  void gravitinoTransportLeavesFinalTableIdentityToNormalizer() {
    String oldEndpoint = System.getProperty("openlineage.transport.endpoint");
    String oldMetalake = System.getProperty("flink.lineage.metalake");
    try {
      System.setProperty("openlineage.transport.endpoint", "/api/lineage");
      System.setProperty("flink.lineage.metalake", "legacy_metalake");
      SqlDatasetExtractor extractor = new SqlDatasetExtractor("hive_catalog", "gv_hive");
      assertEquals(
          Arrays.asList("hive_catalog", "sales.orders"),
          extractor.identity("gv_hive", "sales", "orders"));
    } finally {
      if (oldEndpoint == null) System.clearProperty("openlineage.transport.endpoint");
      else System.setProperty("openlineage.transport.endpoint", oldEndpoint);
      if (oldMetalake == null) System.clearProperty("flink.lineage.metalake");
      else System.setProperty("flink.lineage.metalake", oldMetalake);
    }
  }
}
