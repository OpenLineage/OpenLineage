/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.catalog.iceberg;

import static io.openlineage.spark3.agent.lifecycle.plan.catalog.iceberg.IcebergHandler.CATALOG_IMPL;
import static io.openlineage.spark3.agent.lifecycle.plan.catalog.iceberg.IcebergHandler.TYPE;

import io.openlineage.client.utils.DatasetIdentifier;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.CatalogProperties;
import org.apache.spark.sql.SparkSession;

@Slf4j
class RestCatalogTypeHandler extends BaseCatalogTypeHandler {

  private static final String REST_CATALOG_TYPE = "rest";
  private static final String BIGLAKE_CATALOG_URI =
      "https://biglake.googleapis.com/iceberg/v1beta/restcatalog";

  @Override
  String getType() {
    return REST_CATALOG_TYPE;
  }

  @Override
  boolean matchesCatalogType(Map<String, String> catalogConf) {
    return "rest".equalsIgnoreCase(catalogConf.get(TYPE))
        || catalogConf.containsKey(CATALOG_IMPL)
            && catalogConf.get(CATALOG_IMPL).endsWith("RESTCatalog");
  }

  @Override
  @SneakyThrows
  DatasetIdentifier getIdentifier(
      SparkSession session, Map<String, String> catalogConf, String table) {
    String confUri = catalogConf.get(CatalogProperties.URI);
    String uri = new URI(confUri).toString();
    return new DatasetIdentifier(table, uri);
  }

  @Override
  Map<String, String> catalogProperties(Map<String, String> catalogConf) {
    if (BIGLAKE_CATALOG_URI.equals(catalogConf.get(CatalogProperties.URI))) {
      Map<String, String> properties = new HashMap<>();
      properties.put("gcp_project_id", catalogConf.get("header.x-goog-user-project"));
      return properties;
    }
    return super.catalogProperties(catalogConf);
  }
}
