/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.catalog.iceberg;

import static io.openlineage.spark3.agent.lifecycle.plan.catalog.iceberg.IcebergHandler.CATALOG_IMPL;

import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.client.utils.filesystem.FilesystemDatasetUtils;
import java.util.ArrayList;
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
    ArrayList<String> pathComponents = new ArrayList<>(namespace.length + 1);

    int lastIndex = namespace.length - 1;
    for (int i = 0; i < namespace.length; i++) {
      if (i == lastIndex && !namespace[i].endsWith(".db")) {
        pathComponents.add(namespace[i] + ".db");
      } else {
        pathComponents.add(namespace[i]);
      }
    }

    pathComponents.add(identifier.name());
    return new Path(warehouseLocation, String.join(Path.SEPARATOR, pathComponents));
  }

  @Override
  Map<String, String> catalogProperties(Map<String, String> catalogConf) {
    Map<String, String> properties = new HashMap<>();

    // Backward compatibility: prefer official Iceberg keys, fall back to Google's legacy keys
    // TODO: Google plans on using Iceberg Runtime with BigQuery Metastore support instead of their
    //  own implementation. Remove fallback once migration period is complete.
    //  Google docs: https://cloud.google.com/bigquery/docs/configure-blms#configure-with-dataproc
    //  Iceberg code:
    // https://github.com/apache/iceberg/blob/911a486b0eb8f55c2a44c5aa7fe62c2ca23b1d75/bigquery/src/main/java/org/apache/iceberg/gcp/bigquery/BigQueryMetastoreCatalog.java#L62
    properties.put(
        "gcp_project_id",
        catalogConf.getOrDefault("gcp.bigquery.project-id", catalogConf.get("gcp_project")));
    properties.put(
        "gcp_location",
        catalogConf.getOrDefault("gcp.bigquery.location", catalogConf.get("gcp_location")));

    return properties;
  }
}
