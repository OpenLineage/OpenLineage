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
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.Identifier;

abstract class BaseCatalogTypeHandler {

  abstract String getType();

  abstract boolean matchesCatalogType(Map<String, String> catalogConf);

  abstract DatasetIdentifier getIdentifier(
      SparkSession session, Map<String, String> catalogConf, String table);

  Path defaultTableLocation(Path warehouseLocation, Identifier identifier) {
    // namespace1.namespace2.table -> /warehouseLocation/namespace1/namespace2/table
    String[] namespace = identifier.namespace();

    ArrayList<String> pathComponents = new ArrayList<>(namespace.length + 1);
    pathComponents.addAll(Arrays.asList(namespace));
    pathComponents.add(identifier.name());
    return new Path(warehouseLocation, String.join(Path.SEPARATOR, pathComponents));
  }

  Map<String, String> catalogProperties(Map<String, String> catalogConf) {
    return Collections.emptyMap();
  }
}
