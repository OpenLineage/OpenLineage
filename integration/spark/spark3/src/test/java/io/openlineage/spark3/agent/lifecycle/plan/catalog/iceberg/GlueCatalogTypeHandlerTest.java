/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.catalog.iceberg;

import static io.openlineage.spark3.agent.lifecycle.plan.catalog.iceberg.IcebergHandler.CATALOG_IMPL;
import static io.openlineage.spark3.agent.lifecycle.plan.catalog.iceberg.IcebergHandler.TYPE;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class GlueCatalogTypeHandlerTest {

  private final GlueCatalogTypeHandler handler = new GlueCatalogTypeHandler();

  @Test
  void matchesCatalogType_typeGlueWithFederatedGlueId_returnsFalse() {
    Map<String, String> catalogConf = new HashMap<>();
    catalogConf.put(TYPE, "glue");
    catalogConf.put("glue.id", "557690578487:s3tablescatalog/my-bucket");

    assertThat(handler.matchesCatalogType(catalogConf)).isFalse();
  }

  @Test
  void matchesCatalogType_typeGlueWithHadoopCatalogImpl_returnsFalse() {
    Map<String, String> catalogConf = new HashMap<>();
    catalogConf.put(TYPE, "glue");
    catalogConf.put(CATALOG_IMPL, "org.apache.iceberg.hadoop.HadoopCatalog");

    assertThat(handler.matchesCatalogType(catalogConf)).isFalse();
  }

  @Test
  void matchesCatalogType_typeHive_returnsFalse() {
    Map<String, String> catalogConf = new HashMap<>();
    catalogConf.put(TYPE, "hive");

    assertThat(handler.matchesCatalogType(catalogConf)).isFalse();
  }
}
