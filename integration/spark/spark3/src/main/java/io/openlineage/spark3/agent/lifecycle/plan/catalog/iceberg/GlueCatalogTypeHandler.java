/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.catalog.iceberg;

import static io.openlineage.spark.agent.util.PathUtils.GLUE_TABLE_PREFIX;
import static io.openlineage.spark3.agent.lifecycle.plan.catalog.iceberg.IcebergHandler.CATALOG_IMPL;
import static io.openlineage.spark3.agent.lifecycle.plan.catalog.iceberg.IcebergHandler.TYPE;

import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.spark.agent.util.AwsUtils;
import io.openlineage.spark.agent.util.S3TablesUtils;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;

@Slf4j
class GlueCatalogTypeHandler extends BaseCatalogTypeHandler {

  private static final String GLUE_CATALOG_TYPE = "glue";
  private static final String GLUE_CATALOG_IMPL = "org.apache.iceberg.aws.glue.GlueCatalog";

  @Override
  String getType() {
    return GLUE_CATALOG_TYPE;
  }

  // Iceberg CatalogUtil resolves type=glue only when catalog-impl is absent; otherwise this conf
  // misses Glue and falls back to Hive. Expanding on a copy lets the S3 Tables guard see glue.id.
  private static Map<String, String> expandGlueTypeShorthand(Map<String, String> catalogConf) {
    if (catalogConf.containsKey(CATALOG_IMPL)) {
      return catalogConf;
    }
    if (GLUE_CATALOG_TYPE.equalsIgnoreCase(catalogConf.get(TYPE))) {
      Map<String, String> expanded = new HashMap<>(catalogConf);
      expanded.put(CATALOG_IMPL, GLUE_CATALOG_IMPL);
      return expanded;
    }
    return catalogConf;
  }

  @Override
  boolean matchesCatalogType(Map<String, String> catalogConf) {
    Map<String, String> conf = expandGlueTypeShorthand(catalogConf);
    boolean glueImpl =
        conf.containsKey(CATALOG_IMPL) && conf.get(CATALOG_IMPL).endsWith("GlueCatalog");
    if (!glueImpl) {
      return false;
    }
    // Federated S3 Tables on Glue must not get plain Glue lineage; catalog-impl configs are
    // usually claimed by S3TablesCatalogTypeHandler first, but type=glue shorthand may not be yet.
    if (S3TablesUtils.matchesS3TablesCatalogConfig(conf)) {
      log.warn(
          "Glue catalog (catalog-impl or type=glue) has S3 Tables federation signals; "
              + "not handling it as Glue.");
      return false;
    }
    return true;
  }

  @Override
  Optional<DatasetIdentifier.Symlink> getSymlinkIdentifiers(
      SparkSession session, Map<String, String> catalogConf, String table) {
    SparkContext sparkContext = session.sparkContext();
    Optional<String> arn =
        AwsUtils.getGlueArn(sparkContext.getConf(), sparkContext.hadoopConfiguration());
    if (!arn.isPresent()) {
      log.warn("Glue catalog ARN is unavailable; omitting Glue table symlink for table {}.", table);
    }
    return arn.map(
        s ->
            new DatasetIdentifier.Symlink(
                GLUE_TABLE_PREFIX + table.replace(".", "/"),
                s,
                DatasetIdentifier.SymlinkType.TABLE));
  }
}
