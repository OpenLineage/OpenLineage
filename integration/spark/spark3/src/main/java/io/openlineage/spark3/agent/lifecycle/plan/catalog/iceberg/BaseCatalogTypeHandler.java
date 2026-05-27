/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.catalog.iceberg;

import io.openlineage.client.utils.DatasetIdentifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.Identifier;

abstract class BaseCatalogTypeHandler {

  abstract String getType();

  abstract boolean matchesCatalogType(Map<String, String> catalogConf);

  abstract Optional<DatasetIdentifier> getIdentifier(
      SparkSession session, Map<String, String> catalogConf, String table);

  String getFacetType(Map<String, String> catalogConf) {
    return Optional.ofNullable(catalogConf.get(IcebergHandler.TYPE)).orElse(getType());
  }

  /**
   * Optionally supply the primary {@link DatasetIdentifier} directly, bypassing the default
   * table-location-based identity in {@code IcebergHandler.getDatasetIdentifier}. Handlers like S3
   * Tables override this so the user-facing identity matches the catalog (e.g. {@code
   * arn:aws:s3tables:...}) rather than an opaque physical bucket URI.
   */
  Optional<DatasetIdentifier> getPrimaryIdentifier(
      SparkSession session,
      Map<String, String> catalogConf,
      Identifier identifier,
      String catalogName) {
    return Optional.empty();
  }

  Path defaultTableLocation(Path warehouseLocation, Identifier identifier) {
    // namespace1.namespace2.table -> /warehouseLocation/namespace1/namespace2/table
    String[] namespace = identifier.namespace();

    ArrayList<String> pathComponents = new ArrayList<>(namespace.length + 1);
    pathComponents.addAll(Arrays.asList(namespace));
    pathComponents.add(identifier.name());
    return new Path(warehouseLocation, String.join(Path.SEPARATOR, pathComponents));
  }

  boolean shouldOverridePrimary() {
    return false;
  }

  Map<String, String> catalogProperties(Map<String, String> catalogConf) {
    return Collections.emptyMap();
  }
}
