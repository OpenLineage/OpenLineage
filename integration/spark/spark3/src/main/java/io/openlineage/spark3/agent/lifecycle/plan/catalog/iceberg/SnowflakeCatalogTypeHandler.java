/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.catalog.iceberg;

import static io.openlineage.spark3.agent.lifecycle.plan.catalog.iceberg.IcebergHandler.CATALOG_IMPL;
import static io.openlineage.spark3.agent.lifecycle.plan.catalog.iceberg.IcebergHandler.TYPE;

import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.client.utils.SnowflakeUtils;
import java.util.HashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.CatalogProperties;
import org.apache.spark.sql.SparkSession;

@Slf4j
class SnowflakeCatalogTypeHandler extends BaseCatalogTypeHandler {

  private static final String SNOWFLAKE_CATALOG_URI_SUFFIX = ".snowflakecomputing.com";

  @Override
  String getType() {
    return "rest";
  }

  @Override
  boolean matchesCatalogType(Map<String, String> catalogConf) {
    boolean isRestCatalog =
        "rest".equalsIgnoreCase(catalogConf.get(TYPE))
            || catalogConf.getOrDefault(CATALOG_IMPL, "").endsWith("RESTCatalog");
    String uri = catalogConf.getOrDefault(CatalogProperties.URI, "");
    return isRestCatalog && uri.contains(SNOWFLAKE_CATALOG_URI_SUFFIX);
  }

  @Override
  DatasetIdentifier getIdentifier(
      SparkSession session, Map<String, String> catalogConf, String table) {
    String accountIdentifier = SnowflakeUtils.parseAccountIdentifier(catalogConf.get(CatalogProperties.URI));
    log.debug("Getting identifier for Snowflake Iceberg REST catalog, account={}", accountIdentifier);
    return new DatasetIdentifier(table, SnowflakeUtils.SNOWFLAKE_NAMESPACE_PREFIX + accountIdentifier);
  }

  @Override
  Map<String, String> catalogProperties(Map<String, String> catalogConf) {
    String accountIdentifier = SnowflakeUtils.parseAccountIdentifier(catalogConf.get(CatalogProperties.URI));
    Map<String, String> properties = new HashMap<>();
    properties.put("account_identifier", accountIdentifier);
    return properties;
  }
}
