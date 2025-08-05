/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.catalog.iceberg;

import static io.openlineage.spark3.agent.lifecycle.plan.catalog.iceberg.IcebergHandler.TYPE;

import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.spark.agent.util.PathUtils;
import io.openlineage.spark.agent.util.SparkConfUtils;
import io.openlineage.spark3.agent.lifecycle.plan.catalog.MissingDatasetIdentifierCatalogException;
import java.net.URI;
import java.util.Map;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.CatalogProperties;
import org.apache.spark.sql.SparkSession;

@Slf4j
class HiveCatalogTypeHandler extends BaseCatalogTypeHandler {

  private static final String HIVE_CATALOG_TYPE = "hive";

  @Override
  String getType() {
    return HIVE_CATALOG_TYPE;
  }

  @Override
  boolean matchesCatalogType(Map<String, String> catalogConf) {
    return HIVE_CATALOG_TYPE.equalsIgnoreCase(catalogConf.get(TYPE));
  }

  @Override
  @SneakyThrows
  DatasetIdentifier getIdentifier(
      SparkSession session, Map<String, String> catalogConf, String table) {
    URI metastoreUri;
    String confUri = catalogConf.get(CatalogProperties.URI);
    if (confUri == null) {
      metastoreUri =
          SparkConfUtils.getMetastoreUri(session.sparkContext())
              .orElseThrow(() -> new MissingDatasetIdentifierCatalogException(HIVE_CATALOG_TYPE));
    } else {
      metastoreUri = new URI(confUri);
    }
    return new DatasetIdentifier(table, PathUtils.prepareHiveUri(metastoreUri).toString());
  }
}
