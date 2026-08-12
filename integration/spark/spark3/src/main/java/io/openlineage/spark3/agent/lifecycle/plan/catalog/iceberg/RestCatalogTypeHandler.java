/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.catalog.iceberg;

import static io.openlineage.spark3.agent.lifecycle.plan.catalog.iceberg.IcebergHandler.CATALOG_IMPL;
import static io.openlineage.spark3.agent.lifecycle.plan.catalog.iceberg.IcebergHandler.TYPE;

import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.spark.agent.util.PathUtils;
import io.openlineage.spark3.agent.lifecycle.plan.catalog.MissingDatasetIdentifierCatalogException;
import java.net.URI;
import java.util.Map;
import java.util.Optional;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.CatalogProperties;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;

@Slf4j
class RestCatalogTypeHandler extends BaseCatalogTypeHandler {

  protected static final String REST_CATALOG_TYPE = "rest";
  protected static final String BIGLAKE_CATALOG_URI = "https://biglake.googleapis.com/";

  @Override
  String getType() {
    return REST_CATALOG_TYPE;
  }

  @Override
  boolean matchesCatalogType(Map<String, String> catalogConf) {
    return isRestCatalog(catalogConf)
        && !catalogConf.getOrDefault(CatalogProperties.URI, "").startsWith(BIGLAKE_CATALOG_URI);
  }

  @Override
  DatasetIdentifier getPrimaryIdentifier(
      SparkSession session,
      Map<String, String> catalogConf,
      Identifier identifier,
      TableCatalog tableCatalog) {

    Optional<Path> maybeTableLocation = getTableLocation(identifier, tableCatalog);
    String warehouseLocation = catalogConf.get(CatalogProperties.WAREHOUSE_LOCATION);
    if (!maybeTableLocation.isPresent() && warehouseLocation == null) {
      log.debug(
          "The catalog type is 'rest' and the table location and warehouse location is empty. This is likely a table that is being created");
      throw new MissingDatasetIdentifierCatalogException(
          "No table location found. Probably needs to create table first");
    }
    return PathUtils.fromPath(
        maybeTableLocation.orElseGet(
            () -> defaultTableLocation(new Path(warehouseLocation), identifier)));
  }

  @Override
  @SneakyThrows
  Optional<DatasetIdentifier.Symlink> getSymlinkIdentifiers(
      SparkSession session, Map<String, String> catalogConf, String table) {
    String confUri = catalogConf.get(CatalogProperties.URI);
    String uri = new URI(confUri).toString();
    return Optional.of(
        new DatasetIdentifier.Symlink(table, uri, DatasetIdentifier.SymlinkType.TABLE));
  }

  protected static boolean isRestCatalog(Map<String, String> catalogConf) {
    return "rest".equalsIgnoreCase(catalogConf.get(TYPE))
        || catalogConf.containsKey(CATALOG_IMPL)
            && catalogConf.get(CATALOG_IMPL).endsWith("RESTCatalog");
  }
}
