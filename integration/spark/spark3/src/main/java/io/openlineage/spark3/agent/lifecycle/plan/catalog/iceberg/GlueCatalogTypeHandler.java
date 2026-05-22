/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.catalog.iceberg;

import static io.openlineage.spark.agent.util.PathUtils.GLUE_TABLE_PREFIX;
import static io.openlineage.spark3.agent.lifecycle.plan.catalog.iceberg.IcebergHandler.CATALOG_IMPL;

import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.spark.agent.util.AwsUtils;
import java.util.Map;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;

@Slf4j
class GlueCatalogTypeHandler extends BaseCatalogTypeHandler {

  @Override
  String getType() {
    return "glue";
  }

  @Override
  boolean matchesCatalogType(Map<String, String> catalogConf) {
    return catalogConf.containsKey(CATALOG_IMPL)
        && catalogConf.get(CATALOG_IMPL).endsWith("GlueCatalog");
  }

  @Override
  Optional<DatasetIdentifier> getIdentifier(
      SparkSession session, Map<String, String> catalogConf, String table) {
    SparkContext sparkContext = session.sparkContext();
    Optional<String> arn =
        AwsUtils.getGlueArn(sparkContext.getConf(), sparkContext.hadoopConfiguration());
    if (!arn.isPresent()) {
      log.warn("Glue catalog ARN is unavailable; omitting Glue table symlink for table {}.", table);
    }
    return arn.map(s -> new DatasetIdentifier(GLUE_TABLE_PREFIX + table.replace(".", "/"), s));
  }
}
