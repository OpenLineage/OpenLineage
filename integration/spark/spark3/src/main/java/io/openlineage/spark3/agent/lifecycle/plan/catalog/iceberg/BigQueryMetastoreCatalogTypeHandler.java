/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.catalog.iceberg;

import static io.openlineage.spark3.agent.lifecycle.plan.catalog.iceberg.IcebergHandler.CATALOG_IMPL;

import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.client.utils.filesystem.FilesystemDatasetUtils;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.CatalogProperties;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.Identifier;

class BigQueryMetastoreCatalogTypeHandler extends BaseCatalogTypeHandler {

  @Override
  String getType() {
    return "bigquerymetastore";
  }

  @Override
  boolean matchesCatalogType(Map<String, String> catalogConf) {
    return catalogConf.containsKey(CATALOG_IMPL)
        && catalogConf.get(CATALOG_IMPL).endsWith("BigQueryMetastoreCatalog");
  }

  @Override
  DatasetIdentifier getIdentifier(
      SparkSession session, Map<String, String> catalogConf, String table) {
    String warehouseLocation = catalogConf.get(CatalogProperties.WAREHOUSE_LOCATION);
    return FilesystemDatasetUtils.fromLocationAndName(new Path(warehouseLocation).toUri(), table);
  }

  @Override
  Path defaultTableLocation(Path warehouseLocation, Identifier identifier) {
    // namespace1.namespace2.table -> /warehouseLocation/namespace1/namespace2/table
    String[] namespace = identifier.namespace();
    if (namespace.length > 0 && !namespace[namespace.length - 1].endsWith(".db")) {
      namespace[namespace.length - 1] = namespace[namespace.length - 1] + ".db";
    }
    ArrayList<String> pathComponents = new ArrayList<>(namespace.length + 1);
    pathComponents.addAll(Arrays.asList(namespace));
    pathComponents.add(identifier.name());
    return new Path(warehouseLocation, String.join(Path.SEPARATOR, pathComponents));
  }

  @Override
  Map<String, String> catalogProperties(Map<String, String> catalogConf) {
    Map<String, String> properties = new HashMap<>();
    String projectId = catalogConf.get("gcp.bigquery.project-id");
    if (projectId == null || projectId.isEmpty()) {
      projectId = catalogConf.get("gcp_project");
    }
    properties.put("gcp_project_id", projectId);
    properties.put("gcp_location", catalogConf.get("gcp.bigquery.location"));
    return properties;
  }
}
