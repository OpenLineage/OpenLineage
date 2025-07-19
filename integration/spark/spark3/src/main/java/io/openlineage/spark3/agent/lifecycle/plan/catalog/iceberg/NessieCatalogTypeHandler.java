/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.catalog.iceberg;

import static io.openlineage.spark3.agent.lifecycle.plan.catalog.iceberg.IcebergHandler.TYPE;

import io.openlineage.client.utils.DatasetIdentifier;
import java.net.URI;
import java.util.Map;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.CatalogProperties;
import org.apache.spark.sql.SparkSession;

@Slf4j
class NessieCatalogTypeHandler extends BaseCatalogTypeHandler {

  private static final String NESSIE_CATALOG_TYPE = "nessie";

  @Override
  String getType() {
    return NESSIE_CATALOG_TYPE;
  }

  @Override
  boolean matchesCatalogType(Map<String, String> catalogConf) {
    return NESSIE_CATALOG_TYPE.equalsIgnoreCase(catalogConf.get(TYPE));
  }

  @Override
  @SneakyThrows
  DatasetIdentifier getIdentifier(
      SparkSession session, Map<String, String> catalogConf, String table) {
    log.debug("Getting identifier for nessie");
    String confUri = catalogConf.get(CatalogProperties.URI);
    String uri = new URI(confUri).toString();
    return new DatasetIdentifier(table, uri);
  }
}
