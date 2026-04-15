/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.catalog.iceberg;

import static io.openlineage.spark3.agent.lifecycle.plan.catalog.iceberg.IcebergHandler.TYPE;

import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.spark.agent.util.PathUtils;
import io.openlineage.spark.agent.util.SparkConfUtils;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark3.agent.lifecycle.plan.catalog.MissingDatasetIdentifierCatalogException;
import java.net.URI;
import java.util.HashMap;
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

  @Override
  Map<String, String> catalogProperties(
      Map<String, String> catalogConf, OpenLineageContext context) {
    Map<String, String> properties = new HashMap<>();
    context
        .getSparkSession()
        .ifPresent(
            session -> {
              org.apache.spark.SparkConf conf = session.sparkContext().conf();
              SparkConfUtils.findSparkConfigKey(conf, "spark.dataproc.metastore.project-id")
                  .ifPresent(v -> properties.put("spark.dataproc.metastore.project-id", v));
              SparkConfUtils.findSparkConfigKey(conf, "spark.dataproc.metastore.location")
                  .ifPresent(v -> properties.put("spark.dataproc.metastore.location", v));
              SparkConfUtils.findSparkConfigKey(conf, "spark.dataproc.metastore.instanceId")
                  .ifPresent(v -> properties.put("spark.dataproc.metastore.instanceId", v));
            });
    return properties;
  }
}
