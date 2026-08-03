/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.catalog.iceberg;

import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.spark.agent.util.PathUtils;
import java.util.Collections;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;

@Slf4j
class LakehouseRestCatalogTypeHandler extends RestCatalogTypeHandler {

  @Override
  boolean matchesCatalogType(Map<String, String> catalogConf) {
    return isRestCatalog(catalogConf)
        && catalogConf.getOrDefault(CatalogProperties.URI, "").startsWith(BIGLAKE_CATALOG_URI);
  }

  @Override
  DatasetIdentifier getPrimaryIdentifier(
      SparkSession session,
      Map<String, String> catalogConf,
      Identifier identifier,
      TableCatalog tableCatalog) {
    String warehouseLocation = catalogConf.getOrDefault(CatalogProperties.WAREHOUSE_LOCATION, "");
    return PathUtils.fromPath(
        getTableLocation(identifier, tableCatalog)
            .orElse(generateTableLocation(identifier, tableCatalog, warehouseLocation)));
  }

  private Path generateTableLocation(
      Identifier identifier, TableCatalog tableCatalog, String warehouseLocation) {
    if (warehouseLocation.startsWith("bl://")) {
      if (tableCatalog instanceof SparkCatalog) {
        SparkCatalog sparkCatalog = (SparkCatalog) tableCatalog;
        try {
          Map<String, String> namespaceMetadata =
              sparkCatalog.loadNamespaceMetadata(identifier.namespace());
          return new Path(namespaceMetadata.getOrDefault("location", ""), identifier.name());
        } catch (NoSuchNamespaceException e) {
          log.warn("Unable to find namespace metadata for {}", identifier);
        }
      }
    }
    return defaultTableLocation(new Path(warehouseLocation), identifier);
  }

  @Override
  Map<String, String> catalogProperties(Map<String, String> catalogConf) {
    return Collections.singletonMap(
        "gcp_project_id", catalogConf.get("header.x-goog-user-project"));
  }
}
