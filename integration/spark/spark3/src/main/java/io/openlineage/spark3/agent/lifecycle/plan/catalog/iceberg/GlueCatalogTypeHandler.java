/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.catalog.iceberg;

import static io.openlineage.spark3.agent.lifecycle.plan.catalog.iceberg.IcebergHandler.CATALOG_IMPL;

import io.openlineage.client.dataset.Naming;
import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.spark.agent.util.AwsUtils;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;

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
  DatasetIdentifier getIdentifier(
      SparkSession session, Map<String, String> catalogConf, String table) {
    SparkContext sparkContext = session.sparkContext();
    String[] splitTable = table.split(Pattern.quote("."), 2);
    if (splitTable.length != 2) {
      throw new IllegalArgumentException(
          "Invalid table format. Expected 'database.table', got: " + table);
    }
    Optional<Naming.AWSGlue> arn =
        AwsUtils.getGlueName(
            sparkContext.getConf(),
            sparkContext.hadoopConfiguration(),
            splitTable[0],
            splitTable[1]);
    return arn.map(DatasetIdentifier::new).orElse(null);
  }
}
