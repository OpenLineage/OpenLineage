/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.catalog.iceberg;

import static io.openlineage.spark3.agent.lifecycle.plan.catalog.iceberg.IcebergHandler.TYPE;

import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.client.utils.filesystem.FilesystemDatasetUtils;
import java.util.Map;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.CatalogProperties;
import org.apache.spark.sql.SparkSession;

@Slf4j
class HadoopCatalogTypeHandler extends BaseCatalogTypeHandler {

  private static final String HADOOP_CATALOG_TYPE = "hadoop";

  @Override
  String getType() {
    return HADOOP_CATALOG_TYPE;
  }

  @Override
  boolean matchesCatalogType(Map<String, String> catalogConf) {
    return HADOOP_CATALOG_TYPE.equalsIgnoreCase(catalogConf.get(TYPE));
  }

  @Override
  @SneakyThrows
  DatasetIdentifier getIdentifier(
      SparkSession session, Map<String, String> catalogConf, String table) {
    String warehouseLocation = catalogConf.get(CatalogProperties.WAREHOUSE_LOCATION);
    return FilesystemDatasetUtils.fromLocationAndName(new Path(warehouseLocation).toUri(), table);
  }
}
